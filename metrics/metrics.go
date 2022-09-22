package metrics

import (
	"context"
	"fmt"
	"github.com/uber-go/tally/v4"
	promreporter "github.com/uber-go/tally/v4/prometheus"
	"io"
	"net/http"
	"time"
	"zombiezen.com/go/log"
)

type MetricsRegistry struct {
	scope    tally.Scope
	closer   io.Closer
	reporter promreporter.Reporter
	ctx      context.Context
	httpPort int
}

func NewMetricRegistry(httpPort int) *MetricsRegistry {
	r := promreporter.NewReporter(promreporter.Options{})

	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Prefix:         "silod",
		Tags:           map[string]string{},
		CachedReporter: r,
		Separator:      promreporter.DefaultSeparator,
	}, 1*time.Second)

	return &MetricsRegistry{
		scope:    scope,
		closer:   closer,
		reporter: r,
		ctx:      context.Background(),
		httpPort: httpPort,
	}
}

func (r *MetricsRegistry) UpdateTableSyncCount() {
	r.scope.Tagged(map[string]string{}).Counter("table_sync_count").Inc(1)
}

func (r *MetricsRegistry) TimeTableSync(f func() error) error {
	tsw := r.scope.Tagged(map[string]string{}).Timer("table_sync_timer").Start()
	err := f()
	tsw.Stop()
	return err
}

func (r *MetricsRegistry) TimeGRPCEndpoint(id string, f func() (interface{}, error)) (interface{}, error) {
	r.scope.Tagged(map[string]string{"id": id}).Counter("grpc_endpoint_count").Inc(1)
	tsw := r.scope.Tagged(map[string]string{"id": id}).Timer("grpc_endpoint_timer").Start()
	result, err := f()
	tsw.Stop()
	return result, err
}

func (r *MetricsRegistry) TimeGrainInvocation(id string, f func() (interface{}, error)) (interface{}, error) {
	r.scope.Tagged(map[string]string{"id": id}).Counter("grain_invocation_count").Inc(1)
	tsw := r.scope.Tagged(map[string]string{"id": id}).Timer("grain_invocation_timer").Start()
	result, err := f()
	tsw.Stop()
	return result, err
}

func (r *MetricsRegistry) CountReminderInvocation(id string) {
	r.scope.Tagged(map[string]string{"id": id}).Counter("reminder_invocation_count").Inc(1)
	r.scope.Tagged(map[string]string{}).Counter("reminder_invocation_total").Inc(1)
}

func (r *MetricsRegistry) CountRemindersRescheduled(id string) {
	r.scope.Tagged(map[string]string{"id": id}).Counter("reminders_rescheduled_count").Inc(1)
	r.scope.Tagged(map[string]string{}).Counter("reminders_rescheduled_total").Inc(1)
}

func (r *MetricsRegistry) TimeReminderRegistryTick(f func() error) error {
	r.scope.Tagged(map[string]string{}).Counter("reminder_registry_tick").Inc(1)
	tsw := r.scope.Tagged(map[string]string{}).Timer("reminder_registry_tick_time").Start()
	err := f()
	tsw.Stop()
	return err
}

func (r *MetricsRegistry) Serve() error {
	port := r.httpPort
	http.Handle("/metrics", r.reporter.HTTPHandler())
	log.Infof(r.ctx, "Serving 0.0.0.0:%d/metrics", port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		return fmt.Errorf("unable to serve metrics: %v", err)
	} else {
		select {}
	}
}
