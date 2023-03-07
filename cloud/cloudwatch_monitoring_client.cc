#include "cloud/cloudwatch_monitoring_client.h"

#include <aws/core/utils/Outcome.h>
#include <aws/monitoring/CloudWatchClient.h>
#include <aws/monitoring/CloudWatchErrors.h>
#include <aws/monitoring/CloudWatchServiceClientModel.h>
#include <aws/monitoring/model/Dimension.h>
#include <aws/monitoring/model/MetricDatum.h>
#include <aws/monitoring/model/StandardUnit.h>
#include <stdint.h>

#include <memory>
#include <mutex>
#include <utility>

#include "aws/monitoring/model/PutMetricDataRequest.h"
#include "execution/stages/monitoring_client.h"
#include "trantor/utils/LogStream.h"
#include "trantor/utils/Logger.h"
#include "utils/time.h"

namespace Aws {
namespace Client {
class AsyncCallerContext;
}
}  // namespace Aws

namespace delivery {
// This was ported as a constant because we haven't changed its value in over a
// year of being a config field.
const int batch_period_millis = 1'000 * 15;

const std::string monitoring_namespace = "delivery/stats";

// To reduce costs, we don't want to write data to Cloudwatch for every request.
// This state is shared among all of these clients to safely aggregate their
// counts and batch the writes.
struct State {
  std::mutex mutex;
  MonitoringData data;
  uint64_t next_batch_cutoff = millisForDuration();
};
static State static_state_;

Aws::CloudWatch::Model::MetricDatum makeBaseDatum(const std::string& platform) {
  Aws::CloudWatch::Model::MetricDatum ret;
  Aws::CloudWatch::Model::Dimension dimension;
  dimension.SetName("Platform");
  dimension.SetValue(platform);
  ret.AddDimensions(dimension);
  ret.SetUnit(Aws::CloudWatch::Model::StandardUnit::Count);
  return ret;
}

void CloudwatchMonitoringClient::write(const MonitoringData& data) {
  std::unique_lock<std::mutex> guard(static_state_.mutex);
  static_state_.data.request_insertion_count += data.request_insertion_count;
  static_state_.data.feature_count += data.feature_count;

  uint64_t now = millisForDuration();
  // If it's not time for the next batch write, settle for just aggregating
  // counts.
  if (now < static_state_.next_batch_cutoff) {
    return;
  }

  // If it is time for the next batch write:
  // - Steal the shared data
  // - Iterate the cutoff time
  // - Give up the lock before dealing with AWS
  int request_insertion_count = 0;
  std::swap(request_insertion_count,
            static_state_.data.request_insertion_count);
  int feature_count = 0;
  std::swap(feature_count, static_state_.data.feature_count);
  static_state_.next_batch_cutoff = now + batch_period_millis;
  guard.unlock();

  Aws::CloudWatch::Model::PutMetricDataRequest req;
  req.SetNamespace(monitoring_namespace);
  Aws::CloudWatch::Model::MetricDatum request_insertion_count_datum =
      makeBaseDatum(platform_);
  request_insertion_count_datum.SetMetricName("RequestInsertionCountCpp");
  request_insertion_count_datum.SetValue(request_insertion_count);
  req.AddMetricData(std::move(request_insertion_count_datum));
  Aws::CloudWatch::Model::MetricDatum feature_count_datum =
      makeBaseDatum(platform_);
  feature_count_datum.SetMetricName("FeatureCountCpp");
  feature_count_datum.SetValue(feature_count);
  req.AddMetricData(std::move(feature_count_datum));

  client_.PutMetricDataAsync(
      req, [](const Aws::CloudWatch::CloudWatchClient*,
              const Aws::CloudWatch::Model::PutMetricDataRequest&,
              const Aws::CloudWatch::Model::PutMetricDataOutcome& outcome,
              const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
        if (!outcome.IsSuccess()) {
          LOG_ERROR << "Response error from Cloudwatch: "
                    << outcome.GetError().GetMessage();
        }
      });
}
}  // namespace delivery
