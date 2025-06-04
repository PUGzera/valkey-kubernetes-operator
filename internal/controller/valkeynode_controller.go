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
	"time"

	"github.com/google/uuid"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	valkeyv1 "valkey-operator/api/v1"
)

// ValkeyNodeReconciler reconciles a ValkeyNode object
type ValkeyNodeReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	ClientSet *kubernetes.Clientset
}

// +kubebuilder:rbac:groups=valkey.my.domain,resources=valkeynodes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=valkey.my.domain,resources=valkeynodes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=valkey.my.domain,resources=valkeynodes/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ValkeyNode object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *ValkeyNodeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	logger.Info("Start reconcile...")

	valkeyNode := &valkeyv1.ValkeyNode{}

	logger.Info("Fetching ValkeyNode")
	err := r.Get(ctx, req.NamespacedName, valkeyNode)
	if err != nil {
		logger.Error(err, "Failed to get Valkey Node info")
		return ctrl.Result{Requeue: false}, err
	}

	if valkeyNode.Generation == valkeyNode.Status.ObservedGeneration {
		logger.Info(
			fmt.Sprintf(
				"No update to CO, ObservedGeneration: %d, Generation: %d",
				valkeyNode.Status.ObservedGeneration,
				valkeyNode.Generation,
			),
		)
		return ctrl.Result{Requeue: false}, nil
	}

	logger.Info(fmt.Sprintf("Found ValkeyNode %+v", valkeyNode))
	replicas := valkeyNode.Spec.Replicas

	name := fmt.Sprintf("valkey-deployment-%s", uuid.New().String())
	deplTemplate := r.generateValkeyDeployment(name, replicas, valkeyNode)
	logger.Info(fmt.Sprintf("Creating valkey deployment %s", name))
	err = r.createValkeyDeployment(deplTemplate, req.Namespace, ctx)
	if err != nil {
		logger.Error(err, "Failed to create valkey deployment")
		return ctrl.Result{Requeue: false}, err
	}
	err = r.waitForDeploymentReady(req.Namespace, name, time.Minute*5, ctx)
	if err != nil {
		logger.Error(err, "deployment failed to get ready")
		return ctrl.Result{Requeue: false}, err
	}
	pods, err := r.getDeploymentPods(name, req.Namespace, ctx)
	if err != nil {
		logger.Error(err, "failed to get valkey pods")
		return ctrl.Result{Requeue: false}, err
	}
	if len(pods.Items) < 1 {
		err = errors.New("empty pod list")
		logger.Error(err, "found no Valkey pods")
		return ctrl.Result{Requeue: false}, err
	}
	masterPod := pods.Items[0]
	replicaPods := pods.Items[1:]

	/*res, err := valkey_cluster.AddValkeyNode(masterPod.Status.PodIP)
	if err != nil {
		logger.Error(err, "failed to add node to cluster")
		return ctrl.Result{}, err
	}
	logger.Info(res)*/

	nodeState := valkeyv1.ValkeyNodeState{
		DeplName: name,
		Master: valkeyv1.ValkeyPod{
			PodName: masterPod.Name,
			PodIP:   masterPod.Status.PodIP,
		},
		Replicas: []valkeyv1.ValkeyPod{},
	}

	for _, replicaPod := range replicaPods {
		replicaValkeyPod := valkeyv1.ValkeyPod{
			PodName: replicaPod.Name,
			PodIP:   replicaPod.Status.PodIP,
		}
		nodeState.Replicas = append(nodeState.Replicas, replicaValkeyPod)
	}

	valkeyNode.Status.NodeState = nodeState
	valkeyNode.Status.ObservedGeneration = valkeyNode.Generation

	err = r.Status().Update(ctx, valkeyNode)
	if err != nil {
		logger.Error(err, "failed to update valkey node status")
		return ctrl.Result{Requeue: false}, err
	}

	return ctrl.Result{}, nil
}

func (r *ValkeyNodeReconciler) getDeploymentPods(deplName, namespace string, ctx context.Context) (*corev1.PodList, error) {
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

func (r *ValkeyNodeReconciler) generateValkeyDeployment(name string, replication int, valkeyNode *valkeyv1.ValkeyNode) appsv1.Deployment {
	replicas := int32(replication + 1)
	return appsv1.Deployment{
		ObjectMeta: v1.ObjectMeta{
			Name: name,
			OwnerReferences: []v1.OwnerReference{
				{
					APIVersion: valkeyNode.APIVersion,
					Kind:       valkeyNode.Kind,
					Name:       valkeyNode.Name,
					UID:        valkeyNode.UID,
				},
			},
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

func (r *ValkeyNodeReconciler) generateValkeyPodTemplate(app string) corev1.PodTemplateSpec {
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

func (r *ValkeyNodeReconciler) createValkeyDeployment(depl appsv1.Deployment, namespace string, ctx context.Context) error {
	_, err := r.ClientSet.AppsV1().Deployments(namespace).Create(ctx, &depl, v1.CreateOptions{})
	return err
}

func (r *ValkeyNodeReconciler) waitForDeploymentReady(namespace, deploymentName string, timeout time.Duration, ctx context.Context) error {
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
func (r *ValkeyNodeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&valkeyv1.ValkeyNode{}).
		Complete(r)
}
