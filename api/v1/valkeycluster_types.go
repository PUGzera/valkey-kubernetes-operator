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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ValkeyClusterSpec defines the desired state of ValkeyCluster
type ValkeyClusterSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Foo is an example field of ValkeyCluster. Edit valkeycluster_types.go to remove/update
	Masters             int                     `json:"masters"`
	Replications        int                     `json:"replications"`
	Port                int32                   `json:"port,omitempty"`
	ValkeyVersion       string                  `json:"valkeyVersion,omitempty"`
	PodTemplate         *corev1.PodTemplateSpec `json:"podTemplate,omitempty"`
	ValkeyConfigMapName string                  `json:"valkeyConfigMapName,omitempty"`
}

type ValkeyClusterNode struct {
	ID       string `json:"clusterId"`
	Address  string `json:"address"`
	IP       string `json:"ip"`
	IsMaster bool   `json:"isMaster"`
	MasterId string `json:"masterId,omitempty"`
	Status   string `json:"status"`
	Slots    string `json:"slots,omitempty"`
}

type ClusterState = map[string]ValkeyClusterNode

type ClusterStatus struct {
	Available bool `json:"available"`
	// Can't use the ClusterState type because of Operator SDK generation of CRD
	ClusterState map[string]ValkeyClusterNode `json:"clusterState,omitempty"`
}

// ValkeyClusterStatus defines the observed state of ValkeyCluster
type ValkeyClusterStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	ObservedGeneration int64              `json:"observedGeneration"`
	ClusterStatus      *ClusterStatus     `json:"clusterStatus,omitempty"`
	PreviousConfig     *ValkeyClusterSpec `json:"previousConfig,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// ValkeyCluster is the Schema for the valkeyclusters API
type ValkeyCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ValkeyClusterSpec   `json:"spec,omitempty"`
	Status ValkeyClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ValkeyClusterList contains a list of ValkeyCluster
type ValkeyClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ValkeyCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ValkeyCluster{}, &ValkeyClusterList{})
}
