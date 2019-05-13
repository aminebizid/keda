package scalers

import (
	"context"
	"fmt"
	"strconv"

	log "github.com/Sirupsen/logrus"
	v2beta1 "k8s.io/api/autoscaling/v2beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/APIs/external_metrics"
)

const (
	genericAPIMetricName     = "targetMetricValue"
	defaultTargetMetricValue = 5
	externalMetricType       = "External"
	defaultConnectionSetting = "AzureWebJobsStorage"
)

type genericAPIScaler struct {
	metadata *genericAPIMetadata
}

type genericAPIMetadata struct {
	endpoint          string
	targetMetricValue int
}

// NewGenericAPIScaler creates a new GenericAPIScaler
func NewGenericAPIScaler(resolvedEnv, metadata map[string]string) (Scaler, error) {
	meta, err := parseGenericAPIMetadata(metadata, resolvedEnv)
	if err != nil {
		return nil, fmt.Errorf("error parsing generic Api metadata: %s", err)
	}

	return &genericApiScaler{
		metadata: meta,
	}, nil
}

func parseGenericAPIMetadata(metadata, resolvedEnv map[string]string) (*genericApiMetadata, error) {
	meta := genericApiMetadata{}
	meta.targetMetricValue = defaultTargetMetricValue

	if val, ok := metadata[genericApiMetricName]; ok {
		targetMetricValue, err := strconv.Atoi(val)
		if err != nil {
			log.Errorf("Error parsing generi Api metadata %s: %s", genericApiMetricName, err)
		} else {
			meta.targetMetricValue = targetMetricValue
		}
	}

	if val, ok := metadata["endpoint"]; ok {
		meta.endpoint = val
	} else {
		return nil, fmt.Errorf("no endpoint given")
	}

	return &meta, nil
}

// GetScaleDecision is a func
func (s *genericAPIScaler) IsActive(ctx context.Context) (bool, error) {
	length, err := GetGenericApiMetric(ctx, s.metadata.endpoint)

	if err != nil {
		log.Errorf("error %s", err)
		return false, err
	}

	return length > 0, nil
}

func (s *genericAPIScaler) Close() error {
	return nil
}

func (s *genericAPIScaler) GetMetricSpecForScaling() []v2beta1.MetricSpec {
	targetMetricQty := resource.NewQuantity(int64(s.metadata.targetMetricValue), resource.DecimalSI)
	externalMetric := &v2beta1.ExternalMetricSource{MetricName: genericAPIMetricName, TargetAverageValue: targetMetricQty}
	metricSpec := v2beta1.MetricSpec{External: externalMetric, Type: externalMetricType}
	return []v2beta1.MetricSpec{metricSpec}
}

//GetMetrics returns value for a supported metric and an error if there is a problem getting the metric
func (s *genericAPIScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	metricValue, err := GetGenericApiMetric(ctx, s.metadata.endpoint)

	if err != nil {
		log.Errorf("error getting metric value %s", err)
		return []external_metrics.ExternalMetricValue{}, err
	}

	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewQuantity(int64(metricValue), resource.DecimalSI),
		Timestamp:  metav1.Now(),
	}

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}
