# All Trails Assignment

## How To Run

1. Clone the repository locally.
2. On Windows, open cmd and `cd` to the root of this repository.
3. Create a directory called source_data and download the two .tsv files to this location
4. If you don't have virtualenv installed, execute `pip install virtualenv`.
5. Execute `virtualenv venv`
   a. This project was created with Python 3.11.4. If you are using a different version, please upgrade your default version, or install 3.11.4 and prefix this command with `py -3.11`
7. Execute `"venv/Scripts/activate" start`
8. Execute `pip install -r requirements.txt`
9. Create a directory called `output_data`
10. Execute `python main.py` (Expected duration ~2m)
11. Find the completed dataset in the output_data directory

## How It Works

The main block in `main.py` walks through 7 tasks, each of which has it's own function.
The details of the functions are documented in their docstrings.

1. We call `load_data` twice, once for each source dataframe we want to create
2. `merge_dataframes` takes the two dataframes from the previous step and joins them on `Pseudo_User_ID`
3. `clean_dataframe` takes the merged df from the previous step and de-dupes the rows (since we only care about the first recording), renames the columns, and sets appropriate data types
4. `unpack_recording_summary` parses the JSON in the Recording_Summary field to their own fields
5. `compute_calculated_columns` creates `FirstRecordingDurationInHours` for the analysts and also creates a flag to identify and filter out records where the first recording was invalid (before sign-up date)
6. `validate_df` checks for and replaces outliers and checks for uniqueness and null values in particular columns
7. `save_df` takes the final dataset and saves it to disk

## Schema

| Field Name                    | Field Description                                                                  |
|-------------------------------|------------------------------------------------------------------------------------|
| PseudoUserID (PK)             | Unique ID representing a user                                                      |
| AccountSignUpDateTime         | The date that the user signed-up for an account with AllTrails                     |
| ProSubscriptionSignUpDateTime | The date that the user started their initial Pro subscription (date of conversion) |
| FirstRecordingID              | Unique ID given to recording on creation                                           |
| FirstRecordingDateTime        | Timestamp of the user's first recording, based on the given 2017 data              |
| FirstRecordingActivityType    | Activity Type of first recording (e.g. Hiking, Biking, etc)             | 
| FirstRecordingCaloriesBurned  | Calories burned during first recording                                             |
| FirstRecordingDuration  | It was unclear what this field meant, given that we also have total time           |
| FirstRecordingTotalTime       | The total seconds of the first recording                                           |
| FirstRecordingUpdatedDateTime | Timestamp of the first recording's last update                                     |
| FirstRecordingMovingTime | The total seconds spent moving during the first recording                          |
| FirstRecordingAveragePace | FirstRecordingDuration / FirstRecordingTotalDistance                               |
| FirstRecordingAverageSpeed | FirstRecordingTotalDistance / FirstRecordingDuration                               |
| FirstRecordingElevationGain | Total elevation increase throughout the first recording                            |
| FirstRecordingElevationLoss | Total elevation loss throughout the first recording                                |
| InvalidFirstRecordingDateFlag | True if the FirstRecordingDateTime < AccountSignUpDateTime                         |
| FirstRecordingDurationInHours | Total hours between FirstRecordingDateTime and AccountSignUpDateTime               |

The dataset includes records with invalid first recording dates, as well as users who don't have a first recording. 
If performing analysis strictly on users with a valid first recording, filter where InvalidFirstRecordingDateFlag is False
and FirstRecordingDurationInHours is not null.

The current outlier rules in place are:

- FirstRecordingTotalTime is replaced with null if the value is over 18 hours and the activity type is Hiking
- FirstRecordingTotalTime is replaced with null if the value is over 10 days and the activity type is Backpacking
- FirstRecordingAverageSpeed is replaced with null if the value is over 6 and the activity type is Hiking

I figured that each of the numerous metrics associated with a recording would have a different outlier range for each different activity type - for example, an outlier speed is totally different
when you are looking at a hike vs a scenic drive. I created a list called `outliers_list` that holds dictionaries contains 3 data elements - metric, activity_type, and max_value. Then we loop
through each rule and apply it to the data frame dynamically. In the real world, I would work with the analysts and/or experts on this data to create a full list of rules - I only created a few here
as a demonstration. I would also probably store the outlier_rules as a separate json file so that it could be easily modified in the future without touching the production code
(e.g., if new activity types or metrics are created). I'd probably also other checks like min_value and data type, but that didn't appear to be necessary with this data.

## Opportunities and Assumptions

1. If this were a true batch processing job, we would want to design the ingestion of the source tables differently. 
Based on the file suffixes, we presumably get a new file each year. I would maintain a log of either the last run year 
or a list of file suffixes already processed, depending on how data is retrieved from the source. We would only load new
files each time the job ran. I would then create a staging layer in the database with two tables, one for each source table,
with PKs of the user and recording IDs, respectively. Each year when we process the file, we would merge the new data
into this staging layer. Then we would use these tables to do our merge and all other subsequent steps into our final table
Not only does this become critical as the size of the data scales, but it also would improve the data quality. For example,
a user's first recording didn't necessarily happen the same year they made their account.


2. In order to provide greater flexibility to the consumers of this dataset, I would structure this data as a recording fact table
with a users dimension. The recording fact table would have a forward and reverse sequence number per user so that anyone can
quickly select the nth recording per user. Then I'd create a view for the analysts joining these two tables together with a
datediff function between the sign up and recording date. Not only does this enable the user to perform the same analysis on the
second, third, etc recording, but it also allows us to use recording or user data easily in other applications.


3. If I were deploying this as-is in a production environment, I would simply set up a cron schedule to have this job run
at an appropriate cadence, or ideally have it trigger anytime new data is dropped. If the job were more sophisticated
as I outlined in point 1, I would probably leverage a DAG-creation tool like Airflow or AWS Step Functions.
