I finished this challenge using python with Pandas library.

First of all, I processed the record into a dataframe, dropping the unqualified ones. 
Then, sort the dataframe by transaction date, finding the repeat donors one by one.
Given a repeat donor, create or update a dictionary record, 
whose key is the concatenated string of committee id, zip code and transaction year,
 a list of transaction amount as the value.
 Then, the result is calculable.