/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1 "valkey-operator/api/v1"
)

// ValkeyClusterReconciler reconciles a ValkeyCluster object
type ValkeyClusterReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	ClientSet *kubernetes.Clientset
}

// +kubebuilder:rbac:groups=valkey.my.domain,resources=valkeyclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=valkey.my.domain,resources=valkeyclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=valkey.my.domain,resources=valkeyclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ValkeyCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *ValkeyClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Start reconcile...")

	valkeyCluster := &valkeyv1.ValkeyCluster{}

	logger.Info("Fetching ValkeyCluster")
	err := r.Get(ctx, req.NamespacedName, valkeyCluster)
	if err != nil {
		logger.Error(err, "Failed to get Valkey Cluster info")
		return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 10}, err
	}

	logger.Info(fmt.Sprintf("Found ValkeyCluster %+v", valkeyCluster))
	masters := valkeyCluster.Spec.Masters
	replication := valkeyCluster.Spec.Replications

	for range masters {
		name := fmt.Sprintf("valkey-deployment-%s", uuid.New().String())
		deplTemplate := r.generateValkeyDeployment(name, replication)
		logger.Info(fmt.Sprintf("Creating valkey deployment %s", name))
		err = r.createValkeyDeployment(deplTemplate, req.Namespace, ctx)
		if err != nil {
			logger.Error(err, "Failed to create valkey deployment")
			return ctrl.Result{Requeue: true, RequeueAfter: time.Minute * 5}, err
		}
		err = r.waitForDeploymentReady(req.Namespace, name, time.Minute*5, ctx)
		if err != nil {
			logger.Error(err, "deployment failed to get ready")
			return ctrl.Result{Requeue: true, RequeueAfter: time.Minute * 5}, err
		}
		err = r.labelValkeyDeployment(name, req.Namespace, ctx)
		if err != nil {
			logger.Error(err, "failed to label valkey pods")
			return ctrl.Result{Requeue: false, RequeueAfter: time.Minute * 5}, err
		}
		masterIP, replicaIPs, err := r.getValkeyDeploymentIPs(name, req.Namespace, ctx)
		if err != nil {
			logger.Error(err, "failed to get valkey pod IPs")
			return ctrl.Result{Requeue: true, RequeueAfter: time.Minute * 5}, err
		}
		logger.Info(masterIP)
		logger.Info(fmt.Sprintf("%+v", replicaIPs))
	}

	return ctrl.Result{}, nil
}

func (r *ValkeyClusterReconciler) getDeploymentPods(deplName, namespace string, ctx context.Context) (*corev1.PodList, error) {
	depl, err := r.ClientSet.AppsV1().Deployments(namespace).Get(ctx, deplName, v1.GetOptions{})
	if err != nil {
		return nil, err
	}
	selector, ok := depl.Spec.Selector.MatchLabels["app"]
	if !ok {
		return nil, errors.New("unable to get labelselector \"app\"")
	}
	return r.ClientSet.CoreV1().Pods(namespace).List(ctx, v1.ListOptions{LabelSelector: fmt.Sprintf("app=%s", selector)})
}

func (r *ValkeyClusterReconciler) getValkeyDeploymentIPs(deplName, namespace string, ctx context.Context) (string, []string, error) {
	pods, err := r.getDeploymentPods(deplName, namespace, ctx)
	if err != nil {
		return "", nil, err
	}

	if len(pods.Items) < 1 {
		return "", nil, errors.New("Too few pods in Master Replica deployment")
	}

	var masterIP string
	var replicaIPs []string

	for _, pod := range pods.Items {
		if strings.EqualFold(pod.Labels["valkey-role"], "master") {
			masterIP = pod.Status.PodIP
		} else {
			replicaIPs = append(replicaIPs, pod.Status.PodIP)
		}
	}

	return masterIP, replicaIPs, nil
}

func (r *ValkeyClusterReconciler) labelValkeyDeployment(deplName, namespace string, ctx context.Context) error {
	pods, err := r.getDeploymentPods(deplName, namespace, ctx)
	if err != nil {
		return err
	}

	if len(pods.Items) < 1 {
		return errors.New("Too few pods in Master Replica deployment")
	}

	master := pods.Items[0]
	replicas := pods.Items[1:]

	master.ObjectMeta.Labels["valkey-role"] = "master"
	_, err = r.ClientSet.CoreV1().Pods(namespace).Update(ctx, &master, v1.UpdateOptions{})
	if err != nil {
		return err
	}

	for _, replica := range replicas {
		replica.ObjectMeta.Labels["valkey-role"] = "replica"
		_, err = r.ClientSet.CoreV1().Pods(namespace).Update(ctx, &replica, v1.UpdateOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *ValkeyClusterReconciler) generateValkeyDeployment(name string, replication int) appsv1.Deployment {
	replicas := int32(replication + 1)
	return appsv1.Deployment{
		ObjectMeta: v1.ObjectMeta{
			Name: name,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &v1.LabelSelector{
				MatchLabels: map[string]string{
					"type": "valkey-deployment",
					"app":  name,
				},
			},
			Replicas: &replicas,
			Template: r.generateValkeyPodTemplate(name),
		},
	}
}

func (r *ValkeyClusterReconciler) generateValkeyPodTemplate(app string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: v1.ObjectMeta{
			Labels: map[string]string{
				"type": "valkey-deployment",
				"app":  app,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "valkey-server",
					Image: "valkey/valkey:latest",
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: 6379,
						},
					},
					Command: []string{"valkey-server"},
					Args: []string{
						"--cluster-enabled", "yes",
						"--cluster-config-file", "nodes.conf",
						"--cluster-node-timeout", "5000",
						"--appendonly", "yes",
					},
				},
			},
		},
	}
}

func (r *ValkeyClusterReconciler) createValkeyDeployment(depl appsv1.Deployment, namespace string, ctx context.Context) error {
	_, err := r.ClientSet.AppsV1().Deployments(namespace).Create(ctx, &depl, v1.CreateOptions{})
	return err
}

func (r *ValkeyClusterReconciler) waitForDeploymentReady(namespace, deploymentName string, timeout time.Duration, ctx context.Context) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(context.Context) (bool, error) {
		deploy, err := r.ClientSet.AppsV1().Deployments(namespace).Get(ctx, deploymentName, v1.GetOptions{})
		if err != nil {
			return false, err
		}

		if deploy.Status.ReadyReplicas == *deploy.Spec.Replicas {
			return true, nil
		}

		return false, nil
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *ValkeyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&valkeyv1.ValkeyCluster{}).
		Complete(r)
}
