import json
import time

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


aggregated_counts = {}

def get_body(member_group_id, since, until):
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
                facet="log_path",
                sort=LogsAggregateSort(
                    # type=LogsAggregateSortType.MEASURE,
                    order=LogsSortOrder.DESCENDING,
                    # aggregation=LogsAggregationFunction.COUNT,
                ),
                limit=999,
            ),
        ],
    )

    return body


def get_body_with_page(member_group_id, since, until, page):
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
                facet="log_path",
                sort=LogsAggregateSort(
                    # type=LogsAggregateSortType.MEASURE,
                    order=LogsSortOrder.DESCENDING,
                    # aggregation=LogsAggregationFunction.COUNT,
                ),
                limit=999,
            ),
        ],
        page=LogsAggregateRequestPage(cursor=page),
    )

    return body


def get_datadog_logs(member_group_id, since, until, page=None):
    body = None
    if page:
        body = get_body_with_page(member_group_id, since, until, page)
    else:
        body = get_body(member_group_id, since, until)

    configuration = Configuration()

    with ApiClient(configuration) as api_client:
        api_instance = LogsApi(api_client)
        response = api_instance.aggregate_logs(body=body)
        return response.data.buckets, response.meta


def parse_data(data, member_group_id):

    member_group_pattern = r"/\d+"
    draft_pattern = r"/drafts/[^/]+"
    payment_pattern = r'/payment_methods/.*'

    for entry in data:
        log_path = entry["by"]["log_path"]
        count = entry["computes"]["c0"]

        # Replace numbers after 'member_groups/{id}' with '*'
        log_path = re.sub(member_group_pattern, r"/*", log_path)

        # Replace long string ID after 'drafts/' with '*'
        log_path = re.sub(draft_pattern, r"/drafts/*", log_path)

        # Replace long string ID after 'payment_methods/' with '*'
        log_path = re.sub(payment_pattern, r"/payment_methods/*", log_path)

        key = log_path

        aggregated_counts[key] = aggregated_counts.get(key, 0) + count


def process_data(member_group_id, since, until, page=None, page_count=0):
    buckets, meta = get_datadog_logs(member_group_id, since, until, page)
    # catch the nil cursor
    if "page" in meta:
        page = meta.page.after
    else:
        page = None

    print("Page: ", page[-10:])
    bucket_length = len(buckets)
    parse_data(buckets, member_group_id)
    print(f"Page {page_count} processed")

    print("bucket length: ", bucket_length)
    page_count += 1

    if page:
        process_data(member_group_id, since, until, page, page_count)


if __name__ == '__main__':
    print("starting processing")

    process_data(117, "1689435406465", "1692027406465")

    print("finished processing")

    total = sum(aggregated_counts.values())

    print(f"Total logs: {total}")

    with open('output.json', 'w') as file:
        # Serialize the dictionary to JSON and write to the file
        json.dump(aggregated_counts, file, indent=4)  # The 'indent' parameter adds formatting for readability
