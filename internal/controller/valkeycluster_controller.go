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
	"strings"
	"time"

	"github.com/google/uuid"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1 "valkey-operator/api/v1"

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
	Nodes        map[string]*valkeyv1.ValkeyNode
	ClusterState ClusterState
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
		return ctrl.Result{Requeue: false}, nil
	}

	masters := valkeyCluster.Spec.Masters

	for range masters {
		nodeName := fmt.Sprintf("valkey-node-%s", uuid.New().String())
		valkeyNode := &valkeyv1.ValkeyNode{
			ObjectMeta: v1.ObjectMeta{
				Name:      nodeName,
				Namespace: req.Namespace,
				OwnerReferences: []v1.OwnerReference{
					{
						APIVersion: valkeyCluster.APIVersion,
						Kind:       valkeyCluster.Kind,
						Name:       valkeyCluster.Name,
						UID:        valkeyCluster.UID,
					},
				},
				Labels: map[string]string{
					"owner": valkeyCluster.Name,
				},
			},
			Spec: valkeyv1.ValkeyNodeSpec{
				Replicas: valkeyCluster.Spec.Replications,
			},
		}
		err = r.Create(ctx, valkeyNode)
		if err != nil {
			logger.Error(err, "failed to create ValkeyNode")
			return ctrl.Result{Requeue: false}, err
		}

		r.State.Nodes[valkeyNode.Name] = valkeyNode
	}

	masterPodIPs := make([]string, len(r.State.Nodes))
	var randomNode *valkeyv1.ValkeyNode

	for _, valkeyNode := range r.State.Nodes {
		newValkeyNode, err := r.waitForNodeToBeReady(valkeyNode, time.Minute*5, ctx)
		if err != nil {
			logger.Error(err, "waiting for valkeyNode to be ready timed out")
			return ctrl.Result{Requeue: false}, err
		}
		r.State.Nodes[valkeyNode.Name] = newValkeyNode
		masterPodIPs = append(masterPodIPs, newValkeyNode.Status.NodeState.Master.PodIP+":6379")
		if randomNode == nil {
			randomNode = newValkeyNode
		}
	}

	logger.Info(fmt.Sprintf("IPs %+v", masterPodIPs))

	err = r.createCluster(randomNode.Status.NodeState.Master.PodName, req.Namespace, ctx, masterPodIPs)
	if err != nil {
		logger.Error(err, "failed to create Cluster")
		return ctrl.Result{Requeue: false}, err
	}

	clusterState, err := r.getClusterNodes(randomNode.Status.NodeState.Master.PodName, req.Namespace, ctx)
	if err != nil {
		logger.Error(err, "failed to get cluster nodes")
		return ctrl.Result{Requeue: false}, err
	}

	r.State.ClusterState = clusterState

	logger.Info(fmt.Sprintf("CLUSTER STATE %+v", clusterState))

	valkeyCluster.Status.ObservedGeneration = valkeyCluster.Generation

	err = r.Status().Update(ctx, valkeyCluster)
	if err != nil {
		logger.Error(err, "failed to update valkey cluster status")
		return ctrl.Result{Requeue: false}, err
	}

	return ctrl.Result{Requeue: false}, nil
}

func (r *ValkeyClusterReconciler) waitForNodeToBeReady(valkeyNode *valkeyv1.ValkeyNode, timeout time.Duration, ctx context.Context) (*valkeyv1.ValkeyNode, error) {
	err := wait.PollUntilContextTimeout(ctx, 2*time.Second, timeout, true, func(context.Context) (bool, error) {
		newValkeyNode := &valkeyv1.ValkeyNode{}
		namespacedName := types.NamespacedName{
			Name:      valkeyNode.Name,
			Namespace: valkeyNode.Namespace,
		}
		err := r.Get(ctx, namespacedName, newValkeyNode)
		if err != nil {
			return false, err
		}

		if newValkeyNode.Generation == newValkeyNode.Status.ObservedGeneration {
			return true, nil
		}

		return false, nil
	})
	if err != nil {
		return nil, err
	}
	newValkeyNode := &valkeyv1.ValkeyNode{}
	namespacedName := types.NamespacedName{
		Name:      valkeyNode.Name,
		Namespace: valkeyNode.Namespace,
	}
	err = r.Get(ctx, namespacedName, newValkeyNode)
	if err != nil {
		return nil, err
	}

	return newValkeyNode, nil
}

func (r *ValkeyClusterReconciler) createCluster(podName, namespace string, ctx context.Context, addresses []string) error {
	_, err := r.valkeyCommand(podName, namespace, "--cluster", append([]string{"create", "--cluster-yes"}, addresses...), ctx)
	return err
}

func (r *ValkeyClusterReconciler) getClusterNodes(podName, namespace string, ctx context.Context) (ClusterState, error) {
	stdout, err := r.valkeyCommand(podName, namespace, "cluster", []string{"nodes"}, ctx)
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

func (r *ValkeyClusterReconciler) valkeyCommand(podName, namespace, operation string, flags []string, ctx context.Context) (string, error) {
	logger := log.FromContext(ctx)
	binary := "valkey-cli"
	command := []string{binary, operation}
	command = append(command, flags...)

	logger.Info(fmt.Sprintf("COMMAND %+v", command))

	req := r.ClientSet.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
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
