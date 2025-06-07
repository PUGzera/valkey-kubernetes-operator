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
	"net/http"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	valkeyv1 "valkey-operator/api/v1"
	cluster_manager "valkey-operator/internal/cluster-manager"
)

// ValkeyClusterReconciler reconciles a ValkeyCluster object
type ValkeyClusterReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	ClientSet *kubernetes.Clientset
	State     ValkeyClusterMap
	Config    *rest.Config
}

type ValkeyClusterMap = map[string]ValkeyCluster

type ValkeyCluster struct {
	ClusterCR              *valkeyv1.ValkeyCluster
	ClusterManager         *cluster_manager.ValkeyClusterManager
	ClusterSubscribeCancel context.CancelFunc
}

// +kubebuilder:rbac:groups=valkey.my.domain,resources=valkeyclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=valkey.my.domain,resources=valkeyclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=valkey.my.domain,resources=valkeyclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=update;create;patch;get;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=update;create;patch;get;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=update;create;patch;get;delete;list
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=update;create;patch;get;delete;list

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
		return ctrl.Result{}, nil
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

	err = r.onCreate(valkeyCluster)
	if err != nil {
		logger.Error(err, "failed to create valkey cluster")
		return ctrl.Result{}, nil
	}

	err = r.updateClusterCustomResource(ctx, valkeyCluster.Name)
	if err != nil {
		logger.Error(err, "failed to update custom resource status")
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

func (r *ValkeyClusterReconciler) onUpdate(valkeyCluster *valkeyv1.ValkeyCluster) error {
	prevConf := valkeyCluster.Status.PreviousConfig
	if prevConf == nil {
		return errors.New("no previous config")
	}

	state, ok := r.State[valkeyCluster.Name]
	if !ok {
		prevValkeyCluster := &valkeyv1.ValkeyCluster{
			TypeMeta:   valkeyCluster.TypeMeta,
			ObjectMeta: valkeyCluster.ObjectMeta,
			Spec:       *prevConf,
			Status:     valkeyCluster.Status,
		}
		clusterManager, err := cluster_manager.New(r.GenerateOptions(prevValkeyCluster))
		if err != nil {
			return err
		}

		r.State[valkeyCluster.Name] = ValkeyCluster{
			ClusterManager: clusterManager,
			ClusterCR:      prevValkeyCluster,
		}

		state = r.State[valkeyCluster.Name]
	}

	currentConf := valkeyCluster.Spec

	masterDiff := currentConf.Masters - prevConf.Masters
	if masterDiff < 0 {
		state.ClusterManager.ScaleInMaster(masterDiff * -1)
	} else if masterDiff > 0 {
		state.ClusterManager.ScaleOutMaster(masterDiff)
	}

	replicaDiff := currentConf.Replications - prevConf.Replications
	if replicaDiff < 0 {
		state.ClusterManager.ScaleInReplicas(replicaDiff * -1)
	} else if replicaDiff > 0 {
		state.ClusterManager.ScaleOutReplicas(replicaDiff)
	}

	return nil
}

func (r *ValkeyClusterReconciler) onCreate(valkeyCluster *valkeyv1.ValkeyCluster) error {
	clusterManager, err := cluster_manager.New(r.GenerateOptions(valkeyCluster))
	if err != nil {
		return err
	}

	cancelCtx, cancel := context.WithCancel(context.Background())

	r.State[valkeyCluster.Name] = ValkeyCluster{
		ClusterManager:         clusterManager,
		ClusterCR:              valkeyCluster,
		ClusterSubscribeCancel: cancel,
	}

	go clusterManager.Subscribe(cancelCtx, func(cs *cluster_manager.ClusterStatus) error {
		valkeyCluster.Status.ClusterStatus = generateCRClusterStatus(cs)
		return r.Status().Update(cancelCtx, valkeyCluster)
	})()

	return nil
}

func (r *ValkeyClusterReconciler) GenerateOptions(valkeyCluster *valkeyv1.ValkeyCluster) cluster_manager.Options {
	options := cluster_manager.Options{
		Port:         valkeyCluster.Spec.Port,
		Name:         valkeyCluster.Name,
		Namespace:    valkeyCluster.Namespace,
		Masters:      valkeyCluster.Spec.Masters,
		Replications: valkeyCluster.Spec.Replications,
		Owner: v1.OwnerReference{
			APIVersion: valkeyCluster.APIVersion,
			Name:       valkeyCluster.Name,
			Kind:       valkeyCluster.Kind,
			UID:        valkeyCluster.UID,
		},
		ValkeyVersion:       valkeyCluster.Spec.ValkeyVersion,
		ValkeyConfigMapName: valkeyCluster.Spec.ValkeyConfigMapName,
		Config:              r.Config,
	}

	if valkeyCluster.Spec.PodTemplate != nil {
		options.PodTemplate = *valkeyCluster.Spec.PodTemplate
	}

	return options
}

func (r *ValkeyClusterReconciler) ReadyZ(_ *http.Request) error {
	for _, cluster := range r.State {
		if cluster.ClusterCR == nil {
			return errors.New("valkey cluster not created yet")
		}
		if cluster.ClusterManager == nil {
			return errors.New("valkey cluster manager not created yet")
		}
		availableResp := cluster.ClusterManager.ClusterAvailable()
		if !availableResp.Available {
			return fmt.Errorf("node %s unavailable with reason %s", availableResp.NodeAddress, availableResp.Reason)
		}
	}
	return nil
}

func (r *ValkeyClusterReconciler) updateClusterCustomResource(ctx context.Context, name string) error {
	cluster := r.State[name]

	clusterStatus := cluster.ClusterManager.GetClusterStatus()

	valkeyStatus := cluster.ClusterCR.Status

	valkeyStatus.ClusterStatus = generateCRClusterStatus(clusterStatus)

	valkeyStatus.ObservedGeneration = cluster.ClusterCR.Generation

	valkeyStatus.PreviousConfig = &cluster.ClusterCR.Spec

	cluster.ClusterCR.Status = valkeyStatus

	return r.Status().Update(ctx, cluster.ClusterCR)
}

func generateCRClusterStatus(clusterStatus *cluster_manager.ClusterStatus) *valkeyv1.ClusterStatus {
	availabilityInfo := &valkeyv1.ClusterAvailabilityInfo{
		Available:   clusterStatus.Available.Available,
		Reason:      clusterStatus.Available.Reason,
		NodeAddress: clusterStatus.Available.NodeAddress,
	}
	if clusterStatus.State != nil {
		clusterStatusState := make(valkeyv1.ClusterState)

		for name, node := range clusterStatus.State {
			clusterStatusState[name] = valkeyv1.ValkeyClusterNode{
				ID:       node.ID,
				Address:  node.Address,
				IP:       node.IP,
				IsMaster: node.IsMaster,
				MasterId: node.MasterId,
				Status:   node.Status,
				Slots:    node.Slots,
			}
		}
		return &valkeyv1.ClusterStatus{
			Available:    availabilityInfo,
			ClusterState: clusterStatusState,
		}
	}
	return &valkeyv1.ClusterStatus{
		Available: availabilityInfo,
	}
}

func New(client client.Client,
	scheme *runtime.Scheme,
	clientSet *kubernetes.Clientset,
	config *rest.Config) *ValkeyClusterReconciler {
	return &ValkeyClusterReconciler{
		Client:    client,
		Scheme:    scheme,
		ClientSet: clientSet,
		Config:    config,
		State:     make(ValkeyClusterMap),
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ValkeyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&valkeyv1.ValkeyCluster{}).
		Complete(r)
}
