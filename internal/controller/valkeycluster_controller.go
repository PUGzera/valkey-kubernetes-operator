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
	"bytes"
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

type ClusterState = map[string]ValkeyClusterNode

type ValkeyClusterState struct {
	Cluster      *valkeyv1.ValkeyCluster
	StatefulSet  *appsv1.StatefulSet
	ClusterState ClusterState
}

type ValkeyClusterNode struct {
	ID           string `json:"id"`
	Address      string `json:"address"`
	IP           string `json:"ip"`
	Role         string `json:"role"`
	Master       string `json:"master"`
	PingSent     string `json:"ping_sent"`
	PongReceived string `json:"pong_received"`
	ConfigEpoch  string `json:"config_epoch"`
	LinkState    string `json:"link_state"`
	Slots        string `json:"slots,omitempty"`
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

	err = r.createCluster(ctx)
	if err != nil {
		return ctrl.Result{Requeue: false}, err
	}

	clusterState, err := r.getClusterState(ctx)

	if err != nil {
		return ctrl.Result{Requeue: false}, err
	}

	r.State.ClusterState = clusterState

	err = r.updateClusterCustomResource(ctx, true)

	if err != nil {
		return ctrl.Result{Requeue: false}, err
	}

	return ctrl.Result{Requeue: false}, nil
}

func (r *ValkeyClusterReconciler) updateClusterCustomResource(ctx context.Context, success bool) error {
	valkeyCluster := r.State.Cluster
	var valkeyNodes []valkeyv1.ValkeyNode

	for podName, node := range r.State.ClusterState {
		valkeyNode := valkeyv1.ValkeyNode{
			Role:      node.Role,
			Master:    node.Master,
			Address:   node.Address,
			PodName:   podName,
			ClusterId: node.ID,
			Slots:     node.Slots,
		}
		valkeyNodes = append(valkeyNodes, valkeyNode)
	}
	valkeyStatus := valkeyv1.ValkeyClusterStatus{
		ValkeyNodes: valkeyNodes,
	}

	if success {
		valkeyStatus.ObservedGeneration = valkeyCluster.Generation
	}

	valkeyCluster.Status = valkeyStatus

	return r.Status().Update(ctx, valkeyCluster)
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
		address := r.getFQDNFromPodName(pod.Name)
		if withPort {
			address = address + ":" + strconv.Itoa(int(r.State.Cluster.Spec.Port))
		}
		addresses[i] = address
	}

	return addresses, nil
}

func (r *ValkeyClusterReconciler) getFQDNFromPodName(podName string) string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local", podName, r.State.Cluster.Name, r.State.Cluster.Namespace)
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
	podTemplate := r.State.Cluster.Spec.PodTemplate
	if podTemplate == nil {
		podTemplate = &corev1.PodTemplateSpec{}
	}

	if podTemplate.ObjectMeta.Labels == nil {
		podTemplate.ObjectMeta.Labels = make(map[string]string)
	}
	podTemplate.ObjectMeta.Labels["app"] = name

	valkeyVersion := r.State.Cluster.Spec.ValkeyVersion
	if strings.EqualFold(valkeyVersion, "") {
		valkeyVersion = "latest"
	}

	configFromConfigMap := !strings.EqualFold(r.State.Cluster.Spec.ValkeyConfigMapName, "")
	valkeyConfigName := "valkey-config"
	if configFromConfigMap {
		mode := int32(0644)
		configVolume := corev1.Volume{
			Name: valkeyConfigName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: r.State.Cluster.Spec.ValkeyConfigMapName,
					},
					DefaultMode: &mode,
				},
			},
		}
		podTemplate.Spec.Volumes = append(podTemplate.Spec.Volumes, configVolume)
	}
	containerSpec :=
		corev1.Container{
			Name:  "valkey-server",
			Image: fmt.Sprintf("valkey/valkey:%s", valkeyVersion),
			Ports: []corev1.ContainerPort{
				{
					ContainerPort: r.State.Cluster.Spec.Port,
				},
			},
			Env: []corev1.EnvVar{
				{
					Name: "HOSTNAME",
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "metadata.name",
						},
					},
				},
			},
			Command: []string{"/bin/bash", "-c"},
			Args: []string{
				strings.Join([]string{
					"valkey-server",
					"--cluster-enabled", "yes",
					"--cluster-config-file", "nodes.conf",
					"--cluster-node-timeout", "5000",
					"--appendonly", "yes",
					"--port", strconv.Itoa(int(r.State.Cluster.Spec.Port)),
					"--cluster-announce-hostname",
					r.getFQDNFromPodName("${HOSTNAME}"),
				}, " "),
			},
		}
	if configFromConfigMap {
		containerSpec.VolumeMounts = []corev1.VolumeMount{
			{
				ReadOnly:  true,
				Name:      valkeyConfigName,
				MountPath: "/data",
			},
		}
	}
	podTemplate.Spec.Containers = []corev1.Container{
		containerSpec,
	}

	return *podTemplate
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
					Port:       r.State.Cluster.Spec.Port,
					TargetPort: intstr.FromInt32(r.State.Cluster.Spec.Port),
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
		return errors.New("failed to get valkey pods")
	}

	podName := pods.Items[0].Name

	addresses, err := r.getValkeyNodeAddresses(ctx, true)
	if err != nil {
		return nil
	}
	_, _, err = r.valkeyCommand(
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

func (r *ValkeyClusterReconciler) getClusterState(ctx context.Context) (ClusterState, error) {
	pods, err := r.getValkeyNodePods(ctx)
	if err != nil || len(pods.Items) < 1 {
		return nil, errors.New("failed to get valkey pods")
	}

	podName := pods.Items[0].Name

	stdout, _, err := r.valkeyCommand(podName, "cluster", []string{"nodes"}, ctx)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")

	clusterState := make(ClusterState)

	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) < 8 || len(parts) > 9 {
			return nil, errors.New("malformed cluster state data received")
		}

		node := ValkeyClusterNode{
			ID:           parts[0],
			Address:      strings.Split(parts[1], ",")[1],
			IP:           strings.Split(parts[1], ":")[0],
			Role:         strings.Split(parts[2], "myself,")[0],
			Master:       parts[3],
			PingSent:     parts[4],
			PongReceived: parts[5],
			ConfigEpoch:  parts[6],
			LinkState:    parts[7],
			Slots:        "",
		}

		if len(parts) == 9 {
			node.Slots = strings.Join(parts[8:], " ")
		}

		host := strings.Split(node.Address, ".")[0]

		clusterState[host] = node
	}

	return clusterState, nil
}

func (r *ValkeyClusterReconciler) valkeyCommand(podName, operation string, flags []string, ctx context.Context) (string, string, error) {
	binary := "valkey-cli"
	command := []string{binary, "-p", strconv.Itoa(int(r.State.Cluster.Spec.Port)), operation}
	command = append(command, flags...)

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
		return "", "", err
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: bufio.NewWriter(&stdout),
		Stderr: bufio.NewWriter(&stderr),
	})
	if err != nil {
		return "", "", errors.New(stderr.String())
	}

	return stdout.String(), stderr.String(), nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ValkeyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&valkeyv1.ValkeyCluster{}).
		Complete(r)
}
