# Note that this code is based on AIP-76 and subject to change!

@asset(
    ...,
    schedule="@daily",
    partition=PartitionBySequence(["marketing-dwh", "engineering-dwh"]),
)
def dwh_daily_cloud_spend():
    # Code that analyzes cloud spend for one partition