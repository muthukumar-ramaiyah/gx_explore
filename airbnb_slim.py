import great_expectations as gx
print(gx.__version__)

context = gx.get_context(mode="file", project_root_dir="./context")
context.checkpoints.get("airbnb_calendar_ckp").run()
context.checkpoints.get("airbnb_listing_ckp").run()
context.checkpoints.get("airbnb_reviews_ckp").run()
