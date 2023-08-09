from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.logs_api import LogsApi
from datadog_api_client.v2.model.logs_aggregate_request import LogsAggregateRequest
from datadog_api_client.v2.model.logs_aggregate_sort import LogsAggregateSort
from datadog_api_client.v2.model.logs_aggregate_sort_type import LogsAggregateSortType
from datadog_api_client.v2.model.logs_aggregation_function import LogsAggregationFunction
from datadog_api_client.v2.model.logs_compute import LogsCompute
from datadog_api_client.v2.model.logs_aggregate_request_page import LogsAggregateRequestPage
from datadog_api_client.v2.model.logs_compute_type import LogsComputeType
from datadog_api_client.v2.model.logs_group_by import LogsGroupBy
from datadog_api_client.v2.model.logs_query_filter import LogsQueryFilter
from datadog_api_client.v2.model.logs_sort_order import LogsSortOrder
import re


def get_datadog_logs(member_group_id, since, until):
    body = LogsAggregateRequest(
        compute=[
            LogsCompute(
                aggregation=LogsAggregationFunction.COUNT,
                metric="count",
            ),
        ],
        filter=LogsQueryFilter(
            _from=since,
            indexes=[
                  "*",
            ],
            query=f"service:emissary cluster_name:prod* @member_group:{member_group_id}",
            to=until,
        ),
        group_by=[
            LogsGroupBy(
                facet="log_member_group",
                sort=LogsAggregateSort(
                    type=LogsAggregateSortType.MEASURE,
                    order=LogsSortOrder.DESCENDING,
                    aggregation=LogsAggregationFunction.COUNT,
                ),
                limit=100,
            ),
            LogsGroupBy(
                facet="log_path",
                sort=LogsAggregateSort(
                    type=LogsAggregateSortType.MEASURE,
                    order=LogsSortOrder.DESCENDING,
                    aggregation=LogsAggregationFunction.COUNT,
                ),
                limit=100,
            ),
        ],
        page=LogsAggregateRequestPage(),
    )

    configuration = Configuration()

    with ApiClient(configuration) as api_client:
        api_instance = LogsApi(api_client)
        response = api_instance.aggregate_logs(body=body)

        print(response.meta, "data returned!!!")
        return response.data.buckets


def parse_data(data, member_group_id):
    aggregated_counts = {}

    member_group_pattern = r"/\d+"
    draft_pattern = r"/drafts/[^/]+"

    for entry in data:
        log_path = entry["by"]["log_path"]
        count = entry["computes"]["c0"]

        # Replace numbers after 'member_groups/{id}' with '*'
        log_path = re.sub(member_group_pattern, r"/*", log_path)

        # Replace long string ID after 'drafts/' with '*'
        log_path = re.sub(draft_pattern, r"/drafts/*", log_path)

        key = log_path

        aggregated_counts[key] = aggregated_counts.get(key, 0) + count

    for log_path, count in aggregated_counts.items():
        print(f"Log Member Group: 117, Modified Log Path: {log_path}, Count: {count}")


if __name__ == '__main__':
    data = get_datadog_logs(117, "1672521600000", "1699366400000")
    parse_data(data, 117)

