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
	"bufio"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1 "valkey-operator/api/v1"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// ValkeyClusterReconciler reconciles a ValkeyCluster object
type ValkeyClusterReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	ClientSet *kubernetes.Clientset
	State     ValkeyClusterState
	Config    *rest.Config
}

type ClusterState = []ValkeyClusterNode

type ValkeyClusterState struct {
	Cluster      *valkeyv1.ValkeyCluster
	StatefulSet  *appsv1.StatefulSet
	ClusterState *ClusterState
	Port         int32
}

type ValkeyClusterNode struct {
	ID           string   `json:"id"`
	Address      string   `json:"address"`
	Flags        []string `json:"flags"`
	Master       string   `json:"master"`
	PingSent     string   `json:"ping_sent"`
	PongReceived string   `json:"pong_received"`
	ConfigEpoch  string   `json:"config_epoch"`
	LinkState    string   `json:"link_state"`
	Slots        string   `json:"slots,omitempty"`
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
	err := r.Get(ctx, req.NamespacedName, valkeyCluster)
	if err != nil {
		logger.Error(err, "failed to get ValkeyCluster")
		return ctrl.Result{Requeue: false}, err
	}

	if valkeyCluster.Generation == valkeyCluster.Status.ObservedGeneration {
		logger.Info(
			fmt.Sprintf(
				"No update to CO, ObservedGeneration: %d, Generation: %d",
				valkeyCluster.Status.ObservedGeneration,
				valkeyCluster.Generation,
			),
		)
		return ctrl.Result{}, nil
	}

	r.State.Cluster = valkeyCluster
	r.State.Port = 6379

	err = r.createCluster(ctx)
	if err != nil {
		return ctrl.Result{Requeue: false}, nil
	}

	return ctrl.Result{Requeue: false}, nil
}

func (r *ValkeyClusterReconciler) createCluster(ctx context.Context) error {
	err := r.createValkeyClusterKubernetesResources(ctx)
	if err != nil {
		return err
	}

	err = r.createValkeyCluster(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (r *ValkeyClusterReconciler) getValkeyNodeAddresses(ctx context.Context, withPort bool) ([]string, error) {
	pods, err := r.getValkeyNodePods(ctx)
	if err != nil {
		return nil, err
	}

	addresses := make([]string, len(pods.Items))
	for i, pod := range pods.Items {
		address := fmt.Sprintf("%s.%s.%s.svc.cluster.local", pod.Name, r.State.Cluster.Name, r.State.Cluster.Namespace)
		if withPort {
			address = address + ":" + strconv.Itoa(int(r.State.Port))
		}
		addresses[i] = address
	}

	return addresses, nil
}

func (r *ValkeyClusterReconciler) getValkeyNodePods(ctx context.Context) (*corev1.PodList, error) {
	statefulSet := r.State.StatefulSet
	selector, ok := statefulSet.Spec.Selector.MatchLabels["app"]
	if !ok {
		return nil, errors.New("unable to get labelselector \"app\"")
	}
	return r.ClientSet.
		CoreV1().
		Pods(r.State.Cluster.Namespace).
		List(ctx, v1.ListOptions{LabelSelector: fmt.Sprintf("app=%s", selector)})
}

func (r *ValkeyClusterReconciler) createValkeyClusterKubernetesResources(ctx context.Context) error {
	statefulSetTemplate := r.valkeyStatefulSetTemplate()
	serviceTemplate := r.valkeyServiceTemplate()

	statefulSet, err := r.ClientSet.
		AppsV1().
		StatefulSets(r.State.Cluster.Namespace).
		Create(ctx, &statefulSetTemplate, v1.CreateOptions{})
	if err != nil {
		return err
	}

	err = r.waitForStatefulSetToBeReady(statefulSet, time.Minute*5, ctx)
	if err != nil {
		return err
	}

	_, err = r.ClientSet.
		CoreV1().
		Services(r.State.Cluster.Namespace).
		Create(ctx, &serviceTemplate, v1.CreateOptions{})
	if err != nil {
		return err
	}

	r.State.StatefulSet = statefulSet

	return nil
}

func (r *ValkeyClusterReconciler) waitForStatefulSetToBeReady(statefulset *appsv1.StatefulSet, timeout time.Duration, ctx context.Context) error {
	return wait.PollUntilContextTimeout(ctx, time.Second*5, timeout, true, func(ctx context.Context) (done bool, err error) {
		set, err := r.ClientSet.
			AppsV1().
			StatefulSets(r.State.Cluster.Namespace).
			Get(ctx, statefulset.Name, v1.GetOptions{})
		if err != nil {
			return false, err
		}

		if set.Status.ReadyReplicas == *set.Spec.Replicas {
			return true, nil
		}

		return false, nil
	})
}

func (r *ValkeyClusterReconciler) valkeyStatefulSetTemplate() appsv1.StatefulSet {
	masters := r.State.Cluster.Spec.Masters
	replications := r.State.Cluster.Spec.Replications
	name := r.State.Cluster.Name
	replicas := int32(masters + masters*replications)
	return appsv1.StatefulSet{ObjectMeta: v1.ObjectMeta{
		Name: name,
		OwnerReferences: []v1.OwnerReference{
			{
				APIVersion: r.State.Cluster.APIVersion,
				Kind:       r.State.Cluster.Kind,
				Name:       r.State.Cluster.Name,
				UID:        r.State.Cluster.UID,
			},
		},
	},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Selector: &v1.LabelSelector{
				MatchLabels: map[string]string{
					"app": name,
				},
			},
			Replicas: &replicas,
			Template: r.valkeyPodTemplate(name),
		},
	}
}

func (r *ValkeyClusterReconciler) valkeyPodTemplate(name string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: v1.ObjectMeta{
			Labels: map[string]string{
				"app": name,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "valkey-server",
					Image: "valkey/valkey:latest",
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: r.State.Port,
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

func (r *ValkeyClusterReconciler) valkeyServiceTemplate() corev1.Service {
	name := r.State.Cluster.Name
	return corev1.Service{
		ObjectMeta: v1.ObjectMeta{
			Name: name,
			OwnerReferences: []v1.OwnerReference{
				{
					APIVersion: r.State.Cluster.APIVersion,
					Kind:       r.State.Cluster.Kind,
					Name:       r.State.Cluster.Name,
					UID:        r.State.Cluster.UID,
				},
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Protocol:   corev1.ProtocolTCP,
					Port:       r.State.Port,
					TargetPort: intstr.FromInt32(r.State.Port),
				},
			},
			Selector: map[string]string{
				"app": name,
			},
			Type:      corev1.ServiceTypeClusterIP,
			ClusterIP: "None",
		},
	}
}

func (r *ValkeyClusterReconciler) createValkeyCluster(ctx context.Context) error {
	pods, err := r.getValkeyNodePods(ctx)
	if err != nil || len(pods.Items) < 1 {
		return nil
	}

	podName := pods.Items[0].Name

	addresses, err := r.getValkeyNodeAddresses(ctx, true)
	if err != nil {
		return nil
	}
	_, err = r.valkeyCommand(
		podName,
		"--cluster",
		append(
			[]string{
				"create",
				"--cluster-yes",
				"--cluster-replicas",
				strconv.Itoa(r.State.Cluster.Spec.Replications),
			},
			addresses...),
		ctx)
	return err
}

func (r *ValkeyClusterReconciler) getClusterNodes(podName, namespace string, ctx context.Context) (ClusterState, error) {
	stdout, err := r.valkeyCommand(podName, "cluster", []string{"nodes"}, ctx)
	if err != nil {
		return nil, err
	}

	logger := log.FromContext(ctx)

	lines := strings.Split(strings.TrimSpace(stdout), "\n")

	logger.Info(fmt.Sprintf("LINES %+v", lines))

	var clusterState ClusterState

	for _, line := range lines {
		parts := strings.Fields(line)
		logger.Info(fmt.Sprintf("PARTS %+v", parts))
		if len(parts) < 10 {
			return nil, errors.New("malformed cluster state data received")
		}

		node := ValkeyClusterNode{
			ID:           parts[0],
			Address:      strings.Split(parts[1], ":")[0],
			Flags:        strings.Split(parts[2], ","),
			Master:       parts[3],
			PingSent:     parts[4],
			PongReceived: parts[5],
			ConfigEpoch:  parts[6],
			LinkState:    parts[7],
		}

		if len(parts) > 8 {
			node.Slots = strings.Join(parts[8:], " ")
		}

		clusterState = append(clusterState, node)
	}

	return clusterState, nil
}

func (r *ValkeyClusterReconciler) valkeyCommand(podName, operation string, flags []string, ctx context.Context) (string, error) {
	logger := log.FromContext(ctx)
	binary := "valkey-cli"
	command := []string{binary, operation}
	command = append(command, flags...)

	logger.Info(fmt.Sprintf("COMMAND %+v", command))

	req := r.ClientSet.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(r.State.Cluster.Namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command:   []string{"/bin/bash", "-c", strings.Join(command, " ")},
			Container: "valkey-server",
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	// Create executor
	exec, err := remotecommand.NewSPDYExecutor(r.Config, "POST", req.URL())
	if err != nil {
		return "", err
	}

	var stdout, stderr strings.Builder
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: bufio.NewWriter(&stdout),
		Stderr: bufio.NewWriter(&stderr),
	})
	if err != nil {
		logger.Info("STDERR" + stderr.String())
		return "", err
	}
	logger.Info("STDOUT" + stdout.String())
	logger.Info("STDERR" + stderr.String())
	return stdout.String(), nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ValkeyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&valkeyv1.ValkeyCluster{}).
		Complete(r)
}
