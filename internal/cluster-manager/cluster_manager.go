package cluster_manager

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
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

type clusterStatus struct {
	Available bool
	State     clusterState
}

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
	config              *rest.Config

	clientSet    *kubernetes.Clientset
	statefulSet  *appsv1.StatefulSet
	valkeyClient valkey.Client
	available    bool
	ctx          context.Context
}

func New(options Options) (*ValkeyClusterManager, error) {
	if options.Config == nil {
		return nil, errors.New("invalid config")
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
	ValkeyClusterManager.config = options.Config
	ValkeyClusterManager.ctx = context.Background()

	err = ValkeyClusterManager.createCluster()
	if err != nil {
		return nil, err
	}

	err = ValkeyClusterManager.connectToCluster()
	if err != nil {
		return nil, err
	}

	return ValkeyClusterManager, nil
}

func (c *ValkeyClusterManager) createCluster() error {
	err := c.createValkeyClusterKubernetesResources()
	if err != nil {
		return err
	}

	err = c.createValkeyCluster()
	if err != nil {
		return err
	}

	c.available = true

	return nil
}

func (c *ValkeyClusterManager) getValkeyNodeAddresses(withPort bool) ([]string, error) {
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

	err = c.waitForStatefulSetToBeReady(statefulSet, time.Minute*5)
	if err != nil {
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
	return appsv1.StatefulSet{ObjectMeta: v1.ObjectMeta{
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
			Template: c.valkeyPodTemplate(name),
		},
	}
}

func (c *ValkeyClusterManager) valkeyPodTemplate(name string) corev1.PodTemplateSpec {
	podTemplate := c.podTemplate

	if podTemplate.ObjectMeta.Labels == nil {
		podTemplate.ObjectMeta.Labels = make(map[string]string)
	}
	podTemplate.ObjectMeta.Labels["app"] = name

	valkeyVersion := c.valkeyVersion
	if strings.EqualFold(valkeyVersion, "") {
		valkeyVersion = "latest"
	}

	configFromConfigMap := !strings.EqualFold(c.valkeyConfigMapName, "")
	valkeyConfigName := "valkey-config"
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

	addresses, err := c.getValkeyNodeAddresses(true)
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
	addresses, err := c.getValkeyNodeAddresses(true)
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

func (c *ValkeyClusterManager) GetClusterStatus() (*clusterStatus, error) {
	client := c.valkeyClient

	res, err := client.Do(c.ctx, client.B().ClusterNodes().Build()).ToString()
	if err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(res), "\n")

	clusterState := make(clusterState)

	for _, line := range lines {
		parts := strings.Fields(line)
		if len(parts) < 8 || len(parts) > 9 {
			return nil, errors.New("malformed cluster state data received")
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

	available, err := c.ClusterAvailable()
	if err != nil {
		return nil, err
	}

	return &clusterStatus{
		Available: available,
		State:     clusterState,
	}, nil
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
	err = exec.StreamWithContext(c.ctx, remotecommand.StreamOptions{
		Stdout: bufio.NewWriter(&stdout),
		Stderr: bufio.NewWriter(&stderr),
	})
	if err != nil {
		return "", "", errors.New(stderr.String())
	}

	return stdout.String(), stderr.String(), nil
}

func (c *ValkeyClusterManager) ClusterAvailable() (bool, error) {
	clients := c.valkeyClient.Nodes()
	for address, client := range clients {
		res, err := client.Do(c.ctx, client.B().ClusterInfo().Build()).ToString()
		if err != nil {
			return false, err
		}
		lines := strings.Split(strings.TrimSpace(res), "\n")
		if len(lines) < 1 {
			return false, fmt.Errorf("malformed output from CLUSTER INFO from %s", address)
		}
		prefix := "cluster_state:"
		for _, line := range lines {
			if strings.HasPrefix(line, prefix) && !strings.EqualFold(strings.TrimPrefix(line, prefix), "ok") {
				return false, fmt.Errorf("cluster is not ok from %s", address)
			}
		}
	}
	return true, nil
}

func (c *ValkeyClusterManager) ScaleInMaster(masters int) error {
	if false {
		clusterState, err := c.GetClusterStatus()
		if err != nil {
			return err
		}

		state := clusterState.State
		amountNodes := len(state)
		for i := range masters {
			podToBeScaledIn := fmt.Sprintf("%s-%d", c.name, amountNodes-i)
			node, ok := state[podToBeScaledIn]
			if !ok {
				return fmt.Errorf("pod %s was not in valkey cluster", podToBeScaledIn)
			}
			_, err = c.valkeyClient.Do(
				c.ctx,
				c.
					valkeyClient.
					B().
					ClusterForget().
					NodeId(node.ID).
					Build()).ToString()
			if err != nil {
				return err
			}
		}
		newReplicas := *c.statefulSet.Spec.Replicas - int32(masters)
		c.statefulSet.Spec.Replicas = &newReplicas
		_, err = c.clientSet.
			AppsV1().
			StatefulSets(c.namespace).
			Update(c.ctx, c.statefulSet, v1.UpdateOptions{})
		return err
	}
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
