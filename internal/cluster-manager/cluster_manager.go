package cluster_manager

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

type valkeyClusterNode struct {
	ID       string `json:"clusterId"`
	Address  string `json:"address"`
	IP       string `json:"ip"`
	IsMaster bool   `json:"isMaster"`
	MasterId string `json:"masterId,omitempty"`
	Status   string `json:"status"`
	Slots    string `json:"slots,omitempty"`
}

type clusterState = map[string]valkeyClusterNode

type clusterAvailableResp struct {
	Available   bool   `json:"available"`
	Reason      string `json:"reason,omitempty"`
	NodeAddress string `json:"node,omitempty"`
}

type ClusterStatus struct {
	Available clusterAvailableResp `json:"availability"`
	State     clusterState         `json:"clusterState,omitempty"`
}

type section = string

type nodeName = string

type info = map[section]map[string]string

type clusterNodeInfo = map[nodeName]info

type clusterInfo = map[nodeName]map[string]string

type Options struct {
	Port                int32
	Name                string
	Namespace           string
	Masters             int
	Replications        int
	Owner               v1.OwnerReference
	PodTemplate         corev1.PodTemplateSpec
	ValkeyVersion       string
	ValkeyConfigMapName string
	ValkeyPVCName       string
	Config              *rest.Config
}

type ValkeyClusterManager struct {
	port                int32
	name                string
	namespace           string
	masters             int
	replications        int
	owner               v1.OwnerReference
	podTemplate         corev1.PodTemplateSpec
	valkeyVersion       string
	valkeyConfigMapName string
	valkeyPVCName       string
	config              *rest.Config

	clientSet    *kubernetes.Clientset
	statefulSet  *appsv1.StatefulSet
	valkeyClient valkey.Client
	available    bool
	ctx          context.Context
}

func New(options Options) (*ValkeyClusterManager, error) {
	if options.Config == nil {
		return nil, errors.New("invalid config, config is a nil pointer")
	}

	clientSet, err := kubernetes.NewForConfig(options.Config)
	if err != nil {
		return nil, err
	}

	ValkeyClusterManager := &ValkeyClusterManager{}
	ValkeyClusterManager.clientSet = clientSet
	ValkeyClusterManager.port = options.Port
	ValkeyClusterManager.name = options.Name
	ValkeyClusterManager.namespace = options.Namespace
	ValkeyClusterManager.masters = options.Masters
	ValkeyClusterManager.replications = options.Replications
	ValkeyClusterManager.owner = options.Owner
	ValkeyClusterManager.podTemplate = options.PodTemplate
	ValkeyClusterManager.valkeyVersion = options.ValkeyVersion
	ValkeyClusterManager.valkeyConfigMapName = options.ValkeyConfigMapName
	ValkeyClusterManager.valkeyPVCName = options.ValkeyPVCName
	ValkeyClusterManager.config = options.Config
	ValkeyClusterManager.ctx = context.Background()

	if err := ValkeyClusterManager.createCluster(); err != nil {
		return nil, err
	}

	if err := ValkeyClusterManager.connectToCluster(); err != nil {
		return nil, err
	}

	return ValkeyClusterManager, nil
}

func (c *ValkeyClusterManager) createCluster() error {
	if err := c.createValkeyClusterKubernetesResources(); err != nil {
		return err
	}

	if err := c.createValkeyCluster(); err != nil {
		return err
	}

	c.available = true

	return nil
}

func (c *ValkeyClusterManager) getValkeyPodAddresses(withPort bool) ([]string, error) {
	pods, err := c.getValkeyNodePods()
	if err != nil {
		return nil, err
	}

	addresses := make([]string, len(pods.Items))
	for i, pod := range pods.Items {
		address := c.getFQDNFromPodName(pod.Name)
		if withPort {
			address = address + ":" + strconv.Itoa(int(c.port))
		}
		addresses[i] = address
	}

	return addresses, nil
}

func removePortFromAddress(address string) string {
	return strings.Split(address, ":")[0]
}

func (c *ValkeyClusterManager) getFQDNFromPodName(podName string) string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local", podName, c.name, c.namespace)
}

func (c *ValkeyClusterManager) getValkeyNodePods() (*corev1.PodList, error) {
	statefulSet := c.statefulSet
	selector, ok := statefulSet.Spec.Selector.MatchLabels["app"]
	if !ok {
		return nil, errors.New("unable to get labelselector \"app\"")
	}
	return c.clientSet.
		CoreV1().
		Pods(c.namespace).
		List(c.ctx, v1.ListOptions{LabelSelector: fmt.Sprintf("app=%s", selector)})
}

func (c *ValkeyClusterManager) createValkeyClusterKubernetesResources() error {
	statefulSetTemplate := c.valkeyStatefulSetTemplate()
	serviceTemplate := c.valkeyServiceTemplate()

	statefulSet, err := c.clientSet.
		AppsV1().
		StatefulSets(c.namespace).
		Create(c.ctx, &statefulSetTemplate, v1.CreateOptions{})
	if err != nil {
		return err
	}

	if err := c.waitForStatefulSetToBeReady(statefulSet, time.Minute*5); err != nil {
		return err
	}

	_, err = c.clientSet.
		CoreV1().
		Services(c.namespace).
		Create(c.ctx, &serviceTemplate, v1.CreateOptions{})
	if err != nil {
		return err
	}

	c.statefulSet = statefulSet

	return nil
}

func (c *ValkeyClusterManager) waitForStatefulSetToBeReady(statefulset *appsv1.StatefulSet, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(c.ctx, time.Second*5, timeout, true, func(ctx context.Context) (done bool, err error) {
		set, err := c.clientSet.
			AppsV1().
			StatefulSets(c.namespace).
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

func (c *ValkeyClusterManager) valkeyStatefulSetTemplate() appsv1.StatefulSet {
	masters := c.masters
	replications := c.replications
	name := c.name
	replicas := int32(masters + masters*replications)

	statefulSet := appsv1.StatefulSet{ObjectMeta: v1.ObjectMeta{
		Name: name,
		OwnerReferences: []v1.OwnerReference{
			c.owner,
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
			Template: c.valkeyPodTemplate(),
		},
	}
	if strings.EqualFold(c.valkeyPVCName, "") {
		statefulSet.Spec.VolumeClaimTemplates = append(statefulSet.Spec.VolumeClaimTemplates,
			corev1.PersistentVolumeClaim{
				ObjectMeta: v1.ObjectMeta{
					Name: name,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteMany,
					},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
					StorageClassName: nil,
				},
			})
	}
	return statefulSet
}

func (c *ValkeyClusterManager) valkeyPodTemplate() corev1.PodTemplateSpec {
	podTemplate := c.podTemplate
	name := c.name

	if podTemplate.ObjectMeta.Labels == nil {
		podTemplate.ObjectMeta.Labels = make(map[string]string)
	}
	podTemplate.ObjectMeta.Labels["app"] = name

	valkeyVersion := c.valkeyVersion
	if strings.EqualFold(valkeyVersion, "") {
		valkeyVersion = "latest"
	}

	valkeyConfigName := "valkey-config"

	configFromConfigMap := !strings.EqualFold(c.valkeyConfigMapName, "")
	if configFromConfigMap {
		mode := int32(0644)
		configVolume := corev1.Volume{
			Name: valkeyConfigName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: c.valkeyConfigMapName,
					},
					DefaultMode: &mode,
				},
			},
		}
		podTemplate.Spec.Volumes = append(podTemplate.Spec.Volumes, configVolume)
	}
	pvcFromPVCName := !strings.EqualFold(c.valkeyPVCName, "")
	if pvcFromPVCName {
		pvcVolume := corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: c.valkeyPVCName,
					ReadOnly:  false,
				},
			},
		}
		podTemplate.Spec.Volumes = append(podTemplate.Spec.Volumes, pvcVolume)
	}
	containerSpec :=
		corev1.Container{
			Name:  "valkey-server",
			Image: fmt.Sprintf("valkey/valkey:%s", valkeyVersion),
			Ports: []corev1.ContainerPort{
				{
					ContainerPort: c.port,
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
			VolumeMounts: []corev1.VolumeMount{
				{
					ReadOnly:  false,
					Name:      name,
					MountPath: "/data",
				},
			},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{
						Command: []string{
							"/bin/bash",
							"-c",
							strings.Join([]string{
								"valkey-cli",
								"-p",
								strconv.Itoa(int(c.port)),
								"ping",
							}, " "),
						},
					},
				},
			},
			LivenessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{
						Command: []string{
							"/bin/bash",
							"-c",
							strings.Join([]string{
								"valkey-cli",
								"-p",
								strconv.Itoa(int(c.port)),
								"ping",
							}, " "),
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
					"--port", strconv.Itoa(int(c.port)),
					"--cluster-announce-hostname",
					c.getFQDNFromPodName("${HOSTNAME}"),
					"--cluster-preferred-endpoint-type", "hostname",
				}, " "),
			},
		}
	if configFromConfigMap {
		containerSpec.VolumeMounts = append(containerSpec.VolumeMounts, corev1.VolumeMount{
			ReadOnly:  false,
			Name:      valkeyConfigName,
			MountPath: "/data/nodes.conf",
			SubPath:   "nodes.conf",
		})
	}
	podTemplate.Spec.Containers = []corev1.Container{
		containerSpec,
	}

	return podTemplate
}

func (c *ValkeyClusterManager) valkeyServiceTemplate() corev1.Service {
	name := c.name
	return corev1.Service{
		ObjectMeta: v1.ObjectMeta{
			Name: name,
			OwnerReferences: []v1.OwnerReference{
				c.owner,
			},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Protocol:   corev1.ProtocolTCP,
					Port:       c.port,
					TargetPort: intstr.FromInt32(c.port),
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

func (c *ValkeyClusterManager) createValkeyCluster() error {
	pods, err := c.getValkeyNodePods()
	if err != nil || len(pods.Items) < 1 {
		return errors.New("failed to get valkey pods")
	}

	podName := pods.Items[0].Name

	addresses, err := c.getValkeyPodAddresses(true)
	if err != nil {
		return nil
	}
	_, _, err = c.execValkeyPod(
		podName,
		"--cluster",
		append(
			[]string{
				"create",
				"--cluster-yes",
				"--cluster-replicas",
				strconv.Itoa(c.replications),
			},
			addresses...))
	return err
}

func (c *ValkeyClusterManager) connectToCluster() error {
	addresses, err := c.getValkeyPodAddresses(true)
	if err != nil {
		return err
	}

	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: addresses,
		SendToReplicas: func(cmd valkey.Completed) bool {
			return cmd.IsReadOnly()
		},
	})
	if err != nil {
		return err
	}

	c.valkeyClient = client

	return nil
}

func (c *ValkeyClusterManager) GetClusterStatus() ClusterStatus {
	client := c.valkeyClient

	res, err := client.Do(c.ctx, client.B().ClusterNodes().Build()).ToString()
	if err != nil {
		return ClusterStatus{
			Available: clusterAvailableResp{
				Available:   false,
				Reason:      err.Error(),
				NodeAddress: "Cluster",
			},
		}
	}

	clusterState, err := valkeyClusterNodesResponseToClusterState(res)
	if err != nil {
		return ClusterStatus{
			Available: clusterAvailableResp{
				Available:   false,
				Reason:      err.Error(),
				NodeAddress: "Cluster",
			},
		}
	}

	return ClusterStatus{
		Available: c.ClusterAvailable(),
		State:     clusterState,
	}
}

func valkeyClusterNodesResponseToClusterState(clusterNodesResponse string) (clusterState, error) {
	lines := strings.Split(strings.TrimSpace(clusterNodesResponse), "\n")

	clusterState := make(clusterState)

	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) < 8 || len(parts) > 9 {
			return nil, fmt.Errorf("malformed cluster state data received: %+v", parts)
		}

		node := valkeyClusterNode{
			ID:      parts[0],
			Address: strings.Split(parts[1], ",")[1],
			IP:      strings.Split(parts[1], ":")[0],
			Status:  parts[7],
		}

		if strings.EqualFold(strings.TrimPrefix(parts[2], "myself,"), "master") {
			node.IsMaster = true
		}

		if !strings.EqualFold(parts[3], "-") {
			node.MasterId = parts[3]
		}

		if len(parts) == 9 {
			node.Slots = strings.Join(parts[8:], " ")
		}

		host := strings.Split(node.Address, ".")[0]

		clusterState[host] = node
	}

	return clusterState, nil
}

func (c *ValkeyClusterManager) execValkeyPod(podName, operation string, flags []string) (string, string, error) {
	binary := "valkey-cli"
	command := []string{binary, "-p", strconv.Itoa(int(c.port)), operation}
	command = append(command, flags...)

	req := c.clientSet.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(c.namespace).
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
	exec, err := remotecommand.NewSPDYExecutor(c.config, "POST", req.URL())
	if err != nil {
		return "", "", err
	}

	var stdout, stderr bytes.Buffer
	if err := exec.StreamWithContext(c.ctx, remotecommand.StreamOptions{
		Stdout: bufio.NewWriter(&stdout),
		Stderr: bufio.NewWriter(&stderr),
	}); err != nil {
		return "", "", errors.New(stderr.String())
	}

	return stdout.String(), stderr.String(), nil
}

func (c *ValkeyClusterManager) ClusterAvailable() clusterAvailableResp {
	clients := c.valkeyClient.Nodes()
	for address, client := range clients {
		res, err := client.Do(c.ctx, client.B().ClusterInfo().Build()).ToString()
		if err != nil {
			return clusterAvailableResp{
				Available:   false,
				Reason:      err.Error(),
				NodeAddress: address,
			}
		}
		clusterOk := "cluster_state:ok"
		if !strings.Contains(res, clusterOk) {
			return clusterAvailableResp{
				Available:   false,
				Reason:      res,
				NodeAddress: address,
			}
		}
	}
	return clusterAvailableResp{
		Available: true,
	}
}

func (c *ValkeyClusterManager) ClusterInfo() clusterInfo {
	clusterInfo := make(clusterInfo)
	clients := c.valkeyClient.Nodes()
	for address, client := range clients {
		res, err := client.Do(c.ctx, client.B().ClusterInfo().Build()).ToString()
		if err != nil {
			continue
		}
		clusterInfo[removePortFromAddress(address)] = parseValkeyClusterInfo(res)
	}

	return clusterInfo
}

func parseValkeyClusterInfo(valkeyClusterInfo string) map[string]string {
	clusterInfoMap := make(map[string]string)
	lines := strings.Split(valkeyClusterInfo, "\n")
	for _, line := range lines {
		keyValue := strings.Split(strings.TrimSpace(line), ":")
		if len(keyValue) == 2 {
			clusterInfoMap[keyValue[0]] = keyValue[1]
		}
	}
	return clusterInfoMap
}

func (c *ValkeyClusterManager) ScaleInMaster(masters int) error {
	return nil
}

func (c *ValkeyClusterManager) ScaleOutMaster(masters int) error {
	return nil
}

func (c *ValkeyClusterManager) ScaleInReplicas(replicas int) error {
	return nil
}

func (c *ValkeyClusterManager) ScaleOutReplicas(replicas int) error {
	return nil
}
func (c *ValkeyClusterManager) OnClusterStatusChangeFunc(ctx context.Context, callback func(cs ClusterStatus, ps *ClusterStatus) error) func() error {
	return func() error {
		var lastClusterStatus *ClusterStatus
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				time.Sleep(20 * time.Second)
				clusterStatus := c.GetClusterStatus()
				if lastClusterStatus == nil || !reflect.DeepEqual(*lastClusterStatus, clusterStatus) {
					if err := callback(clusterStatus, lastClusterStatus); err != nil {
						return err
					}
				}
				lastClusterStatus = &clusterStatus
			}
		}
	}
}

func (c *ValkeyClusterManager) Info() clusterNodeInfo {
	clusterInfo := make(clusterNodeInfo)
	clients := c.valkeyClient.Nodes()

	for address, client := range clients {
		res, err := client.Do(c.ctx, client.B().Info().Build()).ToString()
		if err != nil {
			continue
		}
		info, err := parseValkeyInfo(res)
		if err != nil {
			continue
		}
		clusterInfo[removePortFromAddress(address)] = info
	}
	return clusterInfo
}

func parseValkeyInfo(valkeyInfo string) (info, error) {
	info := make(info)
	sections := strings.Split(valkeyInfo, "#")
	for _, section := range sections {
		sectionData := strings.Split(section, "\n")
		if len(sectionData) < 1 {
			return nil, errors.New("malformed info data")
		}
		sectionName := strings.ToLower(strings.TrimSpace(sectionData[0]))
		if len(sectionName) < 1 {
			continue
		}
		metrics := sectionData[1:]
		metricsMap := make(map[string]string)
		for _, metric := range metrics {
			keyValue := strings.Split(strings.TrimSpace(metric), ":")
			if len(keyValue) == 2 {
				metricsMap[keyValue[0]] = keyValue[1]
			}
		}
		info[sectionName] = metricsMap
	}
	return info, nil
}
