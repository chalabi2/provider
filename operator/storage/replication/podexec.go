package replication

import (
	"context"
	"fmt"
	"io"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// PodCommand is one command run in a pod's container.
type PodCommand struct {
	Namespace string
	Pod       string
	Container string
	Command   []string
	// Stdin feeds the command's standard input; nil gives it none.
	Stdin io.Reader
	// Stdout and Stderr take its output as it is written; nil drops it.
	Stdout io.Writer
	Stderr io.Writer
}

// PodExec runs commands in pod containers through the API server. Input and
// output stream: a command reads its stdin as the caller produces it and
// the caller reads its stdout as the command writes it, so what passes
// through is never held whole in memory, however large.
type PodExec interface {
	Exec(ctx context.Context, cmd PodCommand) error
}

// NewPodExec returns the PodExec that streams over the API server's SPDY
// exec channel.
func NewPodExec(cfg *rest.Config, kc kubernetes.Interface) PodExec {
	return &spdyPodExec{cfg: cfg, kc: kc}
}

type spdyPodExec struct {
	cfg *rest.Config
	kc  kubernetes.Interface
}

func (e *spdyPodExec) Exec(ctx context.Context, cmd PodCommand) error {
	req := e.kc.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(cmd.Namespace).
		Name(cmd.Pod).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: cmd.Container,
			Command:   cmd.Command,
			Stdin:     cmd.Stdin != nil,
			Stdout:    cmd.Stdout != nil,
			Stderr:    cmd.Stderr != nil,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(e.cfg, http.MethodPost, req.URL())
	if err != nil {
		return fmt.Errorf("%w: exec in %s/%s: %s", ErrReplication, cmd.Namespace, cmd.Pod, err.Error())
	}

	return exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  cmd.Stdin,
		Stdout: cmd.Stdout,
		Stderr: cmd.Stderr,
	})
}
