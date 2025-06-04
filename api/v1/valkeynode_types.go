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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ValkeyNodeSpec defines the desired state of ValkeyNode
type ValkeyNodeSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Foo is an example field of ValkeyNode. Edit valkeynode_types.go to remove/update
	Replicas int `json:"replicas,omitempty"`
}

// ValkeyNodeStatus defines the observed state of ValkeyNode
type ValkeyNodeStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	ObservedGeneration int64           `json:"observedGeneration"`
	NodeState          ValkeyNodeState `json:"nodeState"`
}

type ValkeyNodeState struct {
	DeplName string      `json:"deployment"`
	Master   ValkeyPod   `json:"master"`
	Replicas []ValkeyPod `json:"replicas"`
}

type ValkeyPod struct {
	PodName string `json:"pod"`
	PodIP   string `json:"IP"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// ValkeyNode is the Schema for the valkeynodes API
type ValkeyNode struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ValkeyNodeSpec   `json:"spec,omitempty"`
	Status ValkeyNodeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ValkeyNodeList contains a list of ValkeyNode
type ValkeyNodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ValkeyNode `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ValkeyNode{}, &ValkeyNodeList{})
}
