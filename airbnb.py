#%%
# This script demonstrates how to set up a Great Expectations Data Source,
# create Data Assets and Batch Definitions, and validate data against an Expectation Suite.
# It uses a pandas filesystem Data Source to read CSV files from a specified directory.

import great_expectations as gx
from great_expectations.checkpoint import (
    UpdateDataDocsAction,
)
print(gx.__version__)

context = gx.get_context(mode="file", project_root_dir="./context")
###########################################

#%%
# Clean up existing Data Source if needed
# context.delete_datasource("airbnb")
#%%
# Define the Data Source's parameters:
# This path is relative to the `base_directory` of the Data Context.
# add once. else clean every time

ds = context.data_sources.add_pandas_filesystem(name="airbnb",
    base_directory="./data")

c_asset = ds.add_csv_asset(name="calendar")
l_asset = ds.add_csv_asset(name="listings")
r_asset = ds.add_csv_asset(name="reviews")
#%%
# Clean up existing Batch Definitions if needed
# c_asset.delete_batch_definition("my_calendar_batch")
# l_asset.delete_batch_definition("my_listings_batch")
# r_asset.delete_batch_definition("my_reviews_batch")

#%%
# Add Batch Definitions
cbd = c_asset.add_batch_definition_path(name="my_calendar_batch", path="calendar.csv")
lbd = l_asset.add_batch_definition_path(name="my_listings_batch", path="listings.csv")
rbd = r_asset.add_batch_definition_path(name="my_reviews_batch", path="reviews.csv")

cb = cbd.get_batch()
lb = lbd.get_batch()
rb = rbd.get_batch()

#%%
# cb.head()
# lb.head()
# rb.head()
#%%
print(context)
#%%
# Clean up existing Expectation Suites if needed
context.suites.delete("calendar_suite")
#%%
# Validate the Batch against an Expectation Suite
suite = context.suites.add(
    gx.core.expectation_suite.ExpectationSuite(name="calendar_suite")
)
suite.add_expectation(
    gx.expectations.ExpectColumnUniqueValueCountToBeBetween (
        column="listing_id", min_value=1, max_value=2829
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="listing_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="available", mostly=0.80)
)
#%%
# Clean up existing Expectation Suites if needed
# context.suites.delete("listings_suite")
#%%
# Validate the Batch against an Expectation Suite
suite = context.suites.add(
    gx.core.expectation_suite.ExpectationSuite(name="listings_suite")
)
suite.add_expectation(
    gx.expectations.ExpectColumnUniqueValueCountToBeBetween (
        column="listing_url", min_value=1, max_value=2829
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="listing_url")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="summary")
)
#%%
# Clean up existing Expectation Suites if needed
# context.suites.delete("reviews_suite")
#%%
# Validate the Batch against an Expectation Suite
suite = context.suites.add(
    gx.core.expectation_suite.ExpectationSuite(name="reviews_suite")
)
suite.add_expectation(
    gx.expectations.ExpectColumnUniqueValueCountToBeBetween (
        column="listing_id", min_value=1, max_value=2829
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="listing_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="reviewer_name")
)

# %%
# validate the calendar batch
print(cb.head())
#%%
cr = cb.validate(context.suites.get("calendar_suite"))
print(cr)
# %%
# validate the listings batch
lb.head()
#%%
lr = lb.validate(context.suites.get("listings_suite"))
print(lr)
# %%
# validate the reviews batch
rr = rb.validate(context.suites.get("reviews_suite"))
print(rr)
# %%

validation_definition = context.validation_definitions.add(
    gx.core.validation_definition.ValidationDefinition(
        name="airbnb_calendar_vd",
        data=context.data_sources.get("airbnb").assets.get("calendar").get_batch_definition("my_calendar_batch"),
        suite=context.suites.get("calendar_suite"),
    )
)
# %%
validation_definition = context.validation_definitions.add(
    gx.core.validation_definition.ValidationDefinition(
        name="airbnb_listings_vd",
        data=context.get_datasource("airbnb").get_asset("listings").get_batch_definition("my_listings_batch"),
        suite=context.suites.get("listings_suite"),
    )
)
# %%
validation_definition = context.validation_definitions.add(
    gx.core.validation_definition.ValidationDefinition(
        name="airbnb_reviews_vd",
        data=context.get_datasource("airbnb").get_asset("reviews").get_batch_definition("my_reviews_batch"),
        suite=context.suites.get("reviews_suite"),
    )
)
#%%
context.checkpoints.delete("airbnb_calendar_ckp")
# %%

# Create a list of Actions for the Checkpoint to perform
action_list = [
    # This Action updates the Data Docs static website with the Validation
    #   Results after the Checkpoint is run.
    UpdateDataDocsAction(
        name="update_all_data_docs",
    ),
]

checkpoint = context.checkpoints.add(
    gx.checkpoint.checkpoint.Checkpoint(
        name="airbnb_calendar_ckp", 
        validation_definitions=[context.validation_definitions.get("airbnb_calendar_vd")],
        actions=action_list,
        result_format="COMPLETE",
    )
)

#%%
checkpoint.run()
#%%
context.checkpoints.add(
    gx.checkpoint.checkpoint.Checkpoint(
        name="airbnb_listing_ckp", 
        validation_definitions=[context.validation_definitions.get("airbnb_listings_vd")],
        actions=action_list,
    )
)

context.checkpoints.get("airbnb_listing_ck").run()
#%%
context.checkpoints.delete("airbnb_reviews_ckp")
# %%
context.checkpoints.add(
    gx.checkpoint.checkpoint.Checkpoint(
        name="airbnb_reviews_ckp", 
        validation_definitions=[context.validation_definitions.get("airbnb_reviews_vd")],
        actions=action_list,
        result_format="BOOLEAN_ONLY",
    )
)

#%%
context.checkpoints.get("airbnb_reviews_ck").run()
# %%
