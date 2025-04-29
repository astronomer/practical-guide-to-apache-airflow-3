# Note that this code is based on AIP-76 and subject to change!

@asset(..., schedule="@daily", partition=PartitionByInterval("@hourly"))
def hourly_data_generated_daily():
    # Code that generates the data for one hour