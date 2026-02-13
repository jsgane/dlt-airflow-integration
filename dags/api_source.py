import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from airflow.models import Variable


config: RESTAPIConfig = {
    "client": {
        "base_url": "https://api.github.com",
        "auth": {
            "token": Variable.get("github_access_token"),  ##dlt.secrets["sources.access_token"], #<--- we already configured access_token above
        },
        "paginator": "header_link" # <---- set up paginator type
    },
    "resources": [  # <--- list resources
        {
            "name": "issues",
            "endpoint": {
                "path": "repos/dlt-hub/dlt/issues",
                "params": {
                    "state": "open",
                },
            },
        },
        {
            "name": "issue_comments", # <-- here we declare dlt.transformer
            "endpoint": {
                "path": "repos/dlt-hub/dlt/issues/{issue_number}/comments",
                "params": {
                    "issue_number": {
                        "type": "resolve", # <--- use type 'resolve' to resolve {issue_number} for transformer
                        "resource": "issues",
                        "field": "number",
                    },

                },
            },
        },
        {
            "name": "contributors",
            "endpoint": {
                "path": "repos/dlt-hub/dlt/contributors",
          },
        },
        {
            "name": "forks", # <-- here we declare dlt.transformer
            "endpoint": {
                "path": "repos/dlt-hub/dlt/forks",
                "params": {
                    "sort": "oldest", # <--- use type 'resolve' to resolve {issue_number} for transformer
                    "per_page": 100,
                },
                "incremental": {
                    "cursor_path": "created_at",
                    "initial_value": "2026-01-01T00:00:00Z",
                    "row_order": "asc"
                },
            },
        },
    ],
}

github_source = rest_api_source(config)

# apply backfilling to forks
github_source.forks.apply_hints(
    incremental = dlt.sources.incremental(
        "created_at",
        initial_value = "2026-01-01T00:00:00Z",
        end_value = "2026-01-22T00:00:00Z",
        row_order = "asc"
    )
)

if __name__ == "__main__":

    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination="snowflake",
        dataset_name="github_data",
        progress="log"
        #dev_mode=True,
    )

    load_info = pipeline.run(github_source)
    print(load_info)