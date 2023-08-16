import numpy as np
import pandas as pd
import ast
import logging
from datetime import datetime


def load_data(base_file_name, suffix):
    """
    Loads a TSV file into a Pandas dataframe.

    :param base_file_name: source table name, e.g. users, recordings
    :param suffix: 4 digit year representing the file date
    :return: pd.DataFrame
    """
    logger.info('Beginning load_data with base_file_name {} and suffix {} at '.format(base_file_name, suffix) + str(datetime.now()))
    full_file_name = base_file_name + '_' + suffix + '.tsv'
    full_path = './source_data/{}'.format(full_file_name)
    df = pd.read_csv(full_path, sep='\t')

    logger.info('Completing load_data with base_file_name {} and suffix {} at '.format(base_file_name, suffix) + str(datetime.now()))

    return df


def merge_dataframes(users_df, recordings_df):
    """
    Merges the users and recordings table on Pseudo_User_ID and selects only the columns of interest.

    :param users_df: users table, represented as a Pandas dataframe
    :param recordings_df: recordings table, represented as a Pandas dataframe
    :return: pd.DataFrame
    """
    logger.info('Beginning merge_dataframes at ' + str(datetime.now()))

    users_df = users_df.merge(recordings_df, how='left', left_on='Pseudo_User_ID', right_on='Pseudo_User_ID')
    cols = ['Pseudo_User_ID', 'signup_date', 'start_date', 'Recording_ID', 'Date_Time', 'Activity_Type',
            'Recording_Summary']

    df = users_df.loc[:, cols]

    logger.info('Completing merge_dataframes at ' + str(datetime.now()))

    return df


def unpack_recording_summary(df):
    """
    Parses the Recording_Summary field from the provided dataframe, generating a new column for each key in the
    JSON.

    :param df: Merged users and recordings tables, represented as a Pandas dataframe
    :return: pd.DataFrame
    """
    logger.info('Beginning unpack_recording_summary at ' + str(datetime.now()))

    parsed_fields_df = df.loc[df['Recording_Summary'] != {}, 'Recording_Summary'].apply(lambda x: pd.Series(x))

    df = df.merge(parsed_fields_df, how='left', left_index=True, right_index=True).drop('Recording_Summary', axis=1)

    df = df.rename(
        {'calories': 'FirstRecordingCaloriesBurned',
         'duration': 'FirstRecordingDuration',
         'timeTotal': 'FirstRecordingTotalTime',
         'updatedAt': 'FirstRecordingUpdatedDateTime',
         'timeMoving': 'FirstRecordingMovingTime',
         'paceAverage': 'FirstRecordingAveragePace',
         'speedAverage': 'FirstRecordingAverageSpeed',
         'distanceTotal': 'FirstRecordingTotalDistance',
         'elevationGain': 'FirstRecordingElevationGain',
         'elevationLoss': 'FirstRecordingElevationLoss'},
        axis=1
    )
    logger.info('Completing unpack_recording_summary at ' + str(datetime.now()))
    return df


def clean_dataframe(df):
    """
    De-dupes the dataset so that there's only one row per user with their first recording's data. Rename the cols to
    be more user-friendly. Converts certain columns to more appropriate data types.

    :param df: Merged users and recordings tables, represented as a Pandas dataframe
    :return: pd.DataFrame
    """
    logger.info('Beginning clean_dataframe at ' + str(datetime.now()))

    df['RowNumber'] = df.groupby('Pseudo_User_ID')['Date_Time'].rank(method='first', ascending=True)
    df = df.loc[df['RowNumber'] == 1, :].drop('RowNumber', axis=1)

    df = df.rename({
        'Pseudo_User_ID': 'PseudoUserID',
        'signup_date': 'AccountSignUpDateTime',
        'start_date': 'ProSubscriptionSignUpDateTime',
        'Recording_ID': 'FirstRecordingID',
        'Date_Time': 'FirstRecordingDateTime',
        'Activity_Type': 'FirstRecordingActivityType'
    }, axis=1)

    df['AccountSignUpDateTime'] = df['AccountSignUpDateTime'].astype('datetime64[ns]')
    df['ProSubscriptionSignUpDateTime'] = df['ProSubscriptionSignUpDateTime'].astype('datetime64[ns]')
    df['FirstRecordingDateTime'] = df['FirstRecordingDateTime'].astype('datetime64[ns]')

    # This step converts the JSON data into dictionaries rather than strings, which will be easier to parse later
    df['Recording_Summary'] = df['Recording_Summary'].apply(lambda x: ast.literal_eval(x) if type(x) == str else {})

    logger.info('Completing clean_dataframe at ' + str(datetime.now()))
    return df


def compute_calculated_columns(df):
    """
    Defines the IsOutlierFlag and FirstRecordingDurationInHours fields.

    :param df: Merged users and recordings tables, represented as a Pandas DataFrame.
    :return: pd.DataFrame
    """
    logger.info('Beginning compute_calculated_columns at ' + str(datetime.now()))

    df['InvalidFirstRecordingDateFlag'] = df.apply(lambda x: 1 if x['AccountSignUpDateTime'] > x['FirstRecordingDateTime'] else 0, axis=1)
    df['FirstRecordingDurationInHours'] = (df['FirstRecordingDateTime'] - df['AccountSignUpDateTime']) \
                                              .dt.total_seconds() / 3600
    df['FirstRecordingDurationInHours'] = df['FirstRecordingDurationInHours'].astype(int)

    logger.info('Completing compute_calculated_columns at ' + str(datetime.now()))
    return df


def validate_df(df):
    """
    Applies the outlier rules to null out metrics that are above a max value given a particular activity type.

    Checks for uniqueness and nullability on certain columns.

    :param df: Merged users and recordings tables, represented as a Pandas DataFrame.
    :return: pd.DataFrame
    """
    outlier_rules = [
        {
            'activity_type': 'Hiking',
            'metric': 'FirstRecordingTotalTime',
            'max_value': 64800
        },
        {
            'activity_type': 'Backpacking',
            'metric': 'FirstRecordingTotalTime',
            'max_value': 864000
        },
        {
            'activity_type': 'Hiking',
            'metric': 'FirstRecordingAverageSpeed',
            'max_value': 6
        }
    ]

    for rule in outlier_rules:
        activity_type = rule['activity_type']
        metric = rule['metric']
        max_value = rule['max_value']

        rows_impacted = (df['FirstRecordingActivityType'] == activity_type) & (df[metric] > max_value)
        df.loc[rows_impacted, metric] = np.nan

    if not df['PseudoUserID'].is_unique:
        raise exception('PseudoUserID is not unique.')
    if not df['FirstRecordingID'].is_unique:
        raise exception('PseudoUserID is not unique.')
    if df['PseudoUserID'].isnull().values.any():
        raise exception('One or more record does not have a PseudoUserID.')

    return df

def save_df(df):
    """
    Saves the dataframe to the project folder.

    :param df: Final dataframe to save to disk.
    :return: None
    """
    logger.info('Beginning save_df at ' + str(datetime.now()))

    df.to_csv('./output_data/UsersDataset.csv', index=False)

    logger.info('Completing save_df at ' + str(datetime.now()))


if __name__ == '__main__':
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.max_columns', None)

    logger = logging.getLogger()
    logging.basicConfig(level=logging.INFO)

    source_files = ['users', 'recordings']

    for source_file in source_files:
        globals()[source_file + '_df'] = load_data(source_file, '2017')

    merged_df = merge_dataframes(users_df, recordings_df)
    cleaned_df = clean_dataframe(merged_df)
    unpacked_df = unpack_recording_summary(cleaned_df)
    final_df = compute_calculated_columns(unpacked_df)
    validated_df = validate_df(final_df)
    save_df(validated_df)
