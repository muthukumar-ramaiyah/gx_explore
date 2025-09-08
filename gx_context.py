import great_expectations as gx
from great_expectations import expectations as gxe

# Define the Data Source's parameters:
# This path is relative to the `base_directory` of the Data Context.
source_folder = "./data"
data_source_name = "my_filesystem_data_source"
# Define the Data Asset's parameters:
asset_name = "listings"

batch_definition_name = "my_batch_definition"
batch_definition_path = "listings.csv"
suite_name = "my_expectation_suite"
definition_name = "my_validation_definition"
site_name = "my_data_docs_site"

# Define a Data Docs site configuration dictionary
base_directory = "uncommitted/data_docs/local_site/"  # this is the default path (relative to the root folder of the Data Context) but can be changed as required
site_config = {
    "class_name": "SiteBuilder",
    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    "store_backend": {
        "class_name": "TupleFilesystemStoreBackend",
        "base_directory": base_directory,
    },
}

# Automate Data Docs updates with a Checkpoint Action
checkpoint_name = "my_checkpoint"
actions = [
    gx.checkpoint.actions.UpdateDataDocsAction(
        name="update_my_site", site_names=[site_name]
    )
]

suite = gx.ExpectationSuite(name=suite_name)

context = gx.get_context(mode="file", project_root_dir="./context")

# Create the Data Source: first time only
# If the Data Source already exists, you can skip this step.
# data_source = context.data_sources.add_pandas_filesystem(
#     name=data_source_name, base_directory=source_folder
# )

# file_csv_asset = data_source.add_csv_asset(name=asset_name)

# batch_definition = file_csv_asset.add_batch_definition_path(
#     name=batch_definition_name, path=batch_definition_path
# )
# preset_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column="summary")
# suite.add_expectation(preset_expectation)

# context.suites.add(suite)

# batch_definition = (
#     context.data_sources.get(data_source_name)
#     .get_asset(asset_name)
#     .get_batch_definition(batch_definition_name)
# )

# validation_definition = gx.ValidationDefinition(
#     name=definition_name, data=batch_definition, suite=context.suites.get(suite_name)
# )

# context.validation_definitions.add(validation_definition)

# # Add the Data Docs configuration to the Data Context
# context.add_data_docs_site(site_name=site_name, site_config=site_config)

# validation_definition = context.validation_definitions.get(definition_name)

# checkpoint = context.checkpoints.add(
#     gx.Checkpoint(
#         name=checkpoint_name,
#         validation_definitions=[validation_definition],
#         actions=actions,
#     )
# )

# print(context)

# batch = batch_definition.get_batch()

# print(batch.head())

# result = batch.validate(context.suites.get(suite_name))
# print(result)


# result = validation_definition.run()
# print(result)


# Manually build the Data Docs
context.build_data_docs(site_names=site_name)

checkpoint = context.checkpoints.get(checkpoint_name)

result = checkpoint.run()

# View the Data Docs
context.open_data_docs()