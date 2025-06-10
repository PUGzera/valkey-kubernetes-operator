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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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
	Metrics   map[string]*prometheus.GaugeVec
}

type ValkeyNamespacedClusterMap = map[string]ValkeyCluster

type ValkeyClusterMap = map[string]ValkeyNamespacedClusterMap

type ValkeyCluster struct {
	ClusterCR              *valkeyv1.ValkeyCluster
	ClusterManager         *cluster_manager.ValkeyClusterManager
	ClusterSubscribeCancel context.CancelFunc
}

const (
	finalizer = "valkey.my.domain/finalizer"
)

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
	if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
		logger.Error(err, fmt.Sprintf("failed to get ValkeyCluster %s/%s", req.Namespace, req.Name))
		return ctrl.Result{}, nil
	}

	if !valkeyCluster.ObjectMeta.DeletionTimestamp.IsZero() {
		if r.State[valkeyCluster.Namespace] != nil {
			if cluster, ok := r.State[valkeyCluster.Namespace][valkeyCluster.Name]; ok {
				cluster.ClusterSubscribeCancel()
			}
		}
		controllerutil.RemoveFinalizer(valkeyCluster, finalizer)
		if err := r.Update(ctx, valkeyCluster); err != nil {
			logger.Error(err, fmt.Sprintf(
				"failed to remove finalizer for %s/%s",
				valkeyCluster.Namespace,
				valkeyCluster.Name,
			))
			return ctrl.Result{}, nil
		}
		delete(r.State[valkeyCluster.Namespace], valkeyCluster.Name)
		return ctrl.Result{}, nil
	}

	if valkeyCluster.Generation == valkeyCluster.Status.ObservedGeneration {
		logger.Info(
			fmt.Sprintf(
				"No update to CO %s/%s, ObservedGeneration: %d, Generation: %d",
				valkeyCluster.Namespace,
				valkeyCluster.Namespace,
				valkeyCluster.Status.ObservedGeneration,
				valkeyCluster.Generation,
			),
		)
		return ctrl.Result{}, nil
	}

	if err := r.onCreate(valkeyCluster); err != nil {
		logger.Error(err, fmt.Sprintf(
			"failed to create valkey cluster %s/%s",
			valkeyCluster.Namespace,
			valkeyCluster.Name,
		))
		return ctrl.Result{}, nil
	}

	if err := r.updateClusterCustomResource(ctx, *valkeyCluster); err != nil {
		logger.Error(err, "failed to update custom resource status")
		return ctrl.Result{}, nil
	}

	if success := controllerutil.AddFinalizer(valkeyCluster, finalizer); !success {
		logger.Error(errors.New("failed to add finalizer"), fmt.Sprintf(
			"failed to add finalizer for %s/%s",
			valkeyCluster.Namespace,
			valkeyCluster.Name,
		))
		return ctrl.Result{}, nil
	}

	if err := r.Update(ctx, valkeyCluster); err != nil {
		logger.Error(errors.New("failed to add finalizer"), fmt.Sprintf(
			"failed to update CR finalizer for %s/%s",
			valkeyCluster.Namespace,
			valkeyCluster.Name,
		))
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// ToDo: Fix onUpdate logic later
func (r *ValkeyClusterReconciler) onUpdate(valkeyCluster *valkeyv1.ValkeyCluster) error {
	prevConf := valkeyCluster.Status.PreviousConfig
	if prevConf == nil {
		return errors.New("no previous config")
	}

	state, ok := r.State[valkeyCluster.Namespace][valkeyCluster.Name]
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

		r.State[valkeyCluster.Namespace][valkeyCluster.Name] = ValkeyCluster{
			ClusterManager: clusterManager,
			ClusterCR:      prevValkeyCluster,
		}

		state = r.State[valkeyCluster.Namespace][valkeyCluster.Name]
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

	if r.State[valkeyCluster.Namespace] == nil {
		r.State[valkeyCluster.Namespace] = make(ValkeyNamespacedClusterMap)
	}

	r.State[valkeyCluster.Namespace][valkeyCluster.Name] = ValkeyCluster{
		ClusterManager:         clusterManager,
		ClusterCR:              valkeyCluster,
		ClusterSubscribeCancel: cancel,
	}

	go clusterManager.OnClusterStatusChangeFunc(cancelCtx, func(cs cluster_manager.ClusterStatus, ps *cluster_manager.ClusterStatus) error {
		if ps == nil {
			if !cs.Available.Available {
				r.Metrics["unhealthy_cluster_amount_total"].WithLabelValues().Inc()
				r.Metrics["unhealthy_cluster_amount_namespace"].WithLabelValues(valkeyCluster.Namespace).Inc()
			}
			valkeyCluster.Status.Available = cs.Available.Available
			return r.Status().Update(cancelCtx, valkeyCluster)
		}
		if !reflect.DeepEqual(cs.Available, ps.Available) {
			if cs.Available.Available && cs.Available.Available != ps.Available.Available {
				r.Metrics["unhealthy_cluster_amount_total"].WithLabelValues().Dec()
				r.Metrics["unhealthy_cluster_amount_namespace"].WithLabelValues(valkeyCluster.Namespace).Dec()
			} else if cs.Available.Available != ps.Available.Available {
				r.Metrics["unhealthy_cluster_amount_total"].WithLabelValues().Inc()
				r.Metrics["unhealthy_cluster_amount_namespace"].WithLabelValues(valkeyCluster.Namespace).Inc()
			}
			valkeyCluster.Status.Available = cs.Available.Available
			return r.Status().Update(cancelCtx, valkeyCluster)
		}
		return nil
	})()

	r.onCreateMetricsUpdate(valkeyCluster)

	return nil
}

func (r *ValkeyClusterReconciler) onCreateMetricsUpdate(valkeyCluster *valkeyv1.ValkeyCluster) {
	r.Metrics["cluster_amount_total"].WithLabelValues().Inc()
	r.Metrics["cluster_amount_namespace"].WithLabelValues(valkeyCluster.Namespace).Inc()

	r.Metrics["master_amount_total"].WithLabelValues().Add(float64(valkeyCluster.Spec.Masters))
	r.Metrics["master_amount_namespace"].WithLabelValues(valkeyCluster.Namespace).Add(float64(valkeyCluster.Spec.Masters))
	r.Metrics["master_amount_cluster"].WithLabelValues(valkeyCluster.Namespace, valkeyCluster.Name).Add(float64(valkeyCluster.Spec.Masters))

	amountReplicas := valkeyCluster.Spec.Masters * valkeyCluster.Spec.Replications
	r.Metrics["replica_amount_total"].WithLabelValues().Add(float64(amountReplicas))
	r.Metrics["replica_amount_namespace"].WithLabelValues(valkeyCluster.Namespace).Add(float64(amountReplicas))
	r.Metrics["replica_amount_cluster"].WithLabelValues(valkeyCluster.Namespace, valkeyCluster.Name).Add(float64(amountReplicas))

	amountNodes := valkeyCluster.Spec.Masters + valkeyCluster.Spec.Masters*valkeyCluster.Spec.Replications
	r.Metrics["node_amount_total"].WithLabelValues().Add(float64(amountNodes))
	r.Metrics["node_amount_namespace"].WithLabelValues(valkeyCluster.Namespace).Add(float64(amountNodes))
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
		ValkeyPVCName:       valkeyCluster.Spec.ValkeyPVCName,
		Config:              r.Config,
	}

	if valkeyCluster.Spec.PodTemplate != nil {
		options.PodTemplate = *valkeyCluster.Spec.PodTemplate
	}

	return options
}

func (r *ValkeyClusterReconciler) ValkeyInfoHttpHandler() *chi.Mux {
	rootRouter := chi.NewRouter()
	r.setupInfoRoutes(rootRouter)
	r.setupClusterInfoRoutes(rootRouter)
	r.setupClusterStatusRoutes(rootRouter)
	return rootRouter
}

func (r *ValkeyClusterReconciler) setupInfoRoutes(rootRouter *chi.Mux) {
	rootRouter.Route("/info", func(router chi.Router) {
		router.Get("/{namespace}/{name}", func(w http.ResponseWriter, req *http.Request) {
			namespace := req.PathValue("namespace")
			name := req.PathValue("name")
			state, ok := r.State[namespace][name]
			if ok {
				if state.ClusterManager == nil {
					http.Error(w, fmt.Sprintf("Valkey cluster %s not yet setup", name), http.StatusNotFound)
				} else {
					writeJSON(w, state.ClusterManager.Info())
				}
			} else {
				http.Error(w, fmt.Sprintf("No valkey cluster with name %s", name), http.StatusNotFound)
			}
		})

		router.Get("/{namespace}/{name}/{node}", func(w http.ResponseWriter, req *http.Request) {
			namespace := req.PathValue("namespace")
			name := req.PathValue("name")
			state, ok := r.State[namespace][name]
			if ok {
				if state.ClusterManager == nil {
					http.Error(w, fmt.Sprintf("Valkey cluster %s not yet setup", name), http.StatusNotFound)
				} else {
					node := req.PathValue("node")
					writeJSON(w, state.ClusterManager.Info()[node])
				}
			} else {
				http.Error(w, fmt.Sprintf("No valkey cluster with name %s", name), http.StatusNotFound)
			}
		})

		router.Get("/{namespace}/{name}/{node}/{section}", func(w http.ResponseWriter, req *http.Request) {
			namespace := req.PathValue("namespace")
			name := req.PathValue("name")
			state, ok := r.State[namespace][name]
			if ok {
				if state.ClusterManager == nil {
					http.Error(w, fmt.Sprintf("Valkey cluster %s not yet setup", name), http.StatusNotFound)
				} else {
					node := req.PathValue("node")
					section := req.PathValue("section")
					writeJSON(w, state.ClusterManager.Info()[node][section])
				}
			} else {
				http.Error(w, fmt.Sprintf("No valkey cluster with name %s", name), http.StatusNotFound)
			}
		})

		router.Get("/{namespace}/{name}/{node}/{section}/{key}", func(w http.ResponseWriter, req *http.Request) {
			namespace := req.PathValue("namespace")
			name := req.PathValue("name")
			state, ok := r.State[namespace][name]
			if ok {
				if state.ClusterManager == nil {
					http.Error(w, fmt.Sprintf("Valkey cluster %s not yet setup", name), http.StatusNotFound)
				} else {
					node := req.PathValue("node")
					section := req.PathValue("section")
					key := req.PathValue("key")
					writeJSON(w, state.ClusterManager.Info()[node][section][key])
				}
			} else {
				http.Error(w, fmt.Sprintf("No valkey cluster with name %s", name), http.StatusNotFound)
			}
		})
	})
}

func (r *ValkeyClusterReconciler) setupClusterInfoRoutes(rootRouter *chi.Mux) {
	rootRouter.Route("/cluster-info", func(router chi.Router) {
		router.Get("/{namespace}/{name}", func(w http.ResponseWriter, req *http.Request) {
			namespace := req.PathValue("namespace")
			name := req.PathValue("name")
			state, ok := r.State[namespace][name]
			if ok {
				if state.ClusterManager == nil {
					http.Error(w, fmt.Sprintf("Valkey cluster %s not yet setup", name), http.StatusNotFound)
				} else {
					writeJSON(w, state.ClusterManager.ClusterInfo())
				}
			} else {
				http.Error(w, fmt.Sprintf("No valkey cluster with name %s", name), http.StatusNotFound)
			}
		})

		router.Get("/{namespace}/{name}/{node}", func(w http.ResponseWriter, req *http.Request) {
			namespace := req.PathValue("namespace")
			name := req.PathValue("name")
			state, ok := r.State[namespace][name]
			if ok {
				if state.ClusterManager == nil {
					http.Error(w, fmt.Sprintf("Valkey cluster %s not yet setup", name), http.StatusNotFound)
				} else {
					node := req.PathValue("node")
					writeJSON(w, state.ClusterManager.ClusterInfo()[node])
				}
			} else {
				http.Error(w, fmt.Sprintf("No valkey cluster with name %s", name), http.StatusNotFound)
			}
		})

		router.Get("/{namespace}/{name}/{node}/{key}", func(w http.ResponseWriter, req *http.Request) {
			namespace := req.PathValue("namespace")
			name := req.PathValue("name")
			state, ok := r.State[namespace][name]
			if ok {
				if state.ClusterManager == nil {
					http.Error(w, fmt.Sprintf("Valkey cluster %s not yet setup", name), http.StatusNotFound)
				} else {
					node := req.PathValue("node")
					key := req.PathValue("key")
					writeJSON(w, state.ClusterManager.ClusterInfo()[node][key])
				}
			} else {
				http.Error(w, fmt.Sprintf("No valkey cluster with name %s", name), http.StatusNotFound)
			}
		})
	})
}

func (r *ValkeyClusterReconciler) setupClusterStatusRoutes(rootRouter *chi.Mux) {
	rootRouter.Route("/cluster-status", func(router chi.Router) {
		router.Get("/{namespace}/{name}", func(w http.ResponseWriter, req *http.Request) {
			namespace := req.PathValue("namespace")
			name := req.PathValue("name")
			state, ok := r.State[namespace][name]
			if ok {
				if state.ClusterManager == nil {
					http.Error(w, fmt.Sprintf("Valkey cluster %s not yet setup", name), http.StatusNotFound)
				} else {
					writeJSON(w, state.ClusterManager.GetClusterStatus())
				}
			} else {
				http.Error(w, fmt.Sprintf("No valkey cluster with name %s", name), http.StatusNotFound)
			}
		})
	})
}

func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "failed to encode JSON", http.StatusInternalServerError)
	}
}

func (r *ValkeyClusterReconciler) updateClusterCustomResource(ctx context.Context, valkeyCluster valkeyv1.ValkeyCluster) error {
	cluster := r.State[valkeyCluster.Namespace][valkeyCluster.Name]

	clusterStatus := cluster.ClusterManager.GetClusterStatus()

	valkeyStatus := cluster.ClusterCR.Status

	valkeyStatus.Available = clusterStatus.Available.Available

	valkeyStatus.ObservedGeneration = cluster.ClusterCR.Generation

	valkeyStatus.PreviousConfig = &cluster.ClusterCR.Spec

	cluster.ClusterCR.Status = valkeyStatus

	return r.Status().Update(ctx, cluster.ClusterCR)
}

func New(client client.Client,
	scheme *runtime.Scheme,
	clientSet *kubernetes.Clientset,
	config *rest.Config,
	metrics map[string]*prometheus.GaugeVec) *ValkeyClusterReconciler {
	return &ValkeyClusterReconciler{
		Client:    client,
		Scheme:    scheme,
		ClientSet: clientSet,
		Config:    config,
		Metrics:   metrics,
		State:     make(ValkeyClusterMap),
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *ValkeyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&valkeyv1.ValkeyCluster{}).
		Complete(r)
}
