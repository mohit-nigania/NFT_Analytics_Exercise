# Installation
* configure pyspark in the system , Spark , hadoop ,java and python
* Once the pyspark is up and running its good to go nothing to be install as dependecy

# Run Program 
* Simply run the task.py and we get the two df in the console as result  and the output file in the repo folder


# Function Details

1. read_raw(self)
    Function to read parwuest and create dataframe 
2. remove_duplicates(self)
    Function to remove the duplicate from columns "transaction_timestamp", "from_address", "to_address
3. add_date(self)
    Function to extract date from the timestamp
4. top5_h_amount(self)
    Function to get Daily top-5 NFT tokens with the highest amount
5. top5_am_amount(self)
    Function to get Daily top-5 NFT tokens with the accumulated transaction amounts
6. get_dta(self):
	Function to get df with columns date, token_id, amount and save to parquet file
7. get_dta(self):
	Function to get df with columns date, token_id, accumulated amount and save to parquet file
8. process
    Function to RUN all the functions
