
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

@dlt.source
def jaffles_source(parallelized=True, write_disposition='replace'):
    client = RESTClient(
        base_url="https://jaffle-shop.scalevector.ai/api/v1/",
        paginator=PageNumberPaginator(
            base_page=1,
            page=None,
            page_param='page',
            total_path=None,
            maximum_page=None,
            stop_after_empty_page=True
        ),
    )

    @dlt.resource
    def jaffles_customers(parallelized=parallelized, write_disposition=write_disposition):
        for page in client.paginate("customers"):
            yield page

    @dlt.resource
    def jaffles_orders(parallelized=parallelized, write_disposition=write_disposition):
        for page in client.paginate("orders"):
            yield page

    @dlt.resource
    def jaffles_products(parallelized=parallelized, write_disposition=write_disposition):
        for page in client.paginate("products"):
            yield page

    return jaffles_customers, jaffles_orders, jaffles_products

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="jaffles_github_action",
        destination="duckdb",
        dataset_name="jaffles_data",
    )

    load_info = pipeline.run(jaffles_source())
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
