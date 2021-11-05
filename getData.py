import boto3
import pandas as pd
from datetime import datetime
from decimal import Decimal
import numpy as np


from boto3.dynamodb.conditions import Key, Attr
dynamodb = boto3.resource('dynamodb')


class GetDataAWS:
    """
    This class extracts distance and motion data from AWS by filtering specific start & end date, location and table name.
    """

    def extract_data(self, start_time, end_time, location, table_name):

        table = dynamodb.Table(table_name)

        start_time, end_time = self.converting_user_input_to_unixtime(start_time, end_time)

        # try:
        response = table.scan(
            FilterExpression=Attr('Timestamp').gte(Decimal(start_time)) & Attr('Timestamp').lte(Decimal(end_time))
        )
        if len(response['Items']) == 0:
            print("There is no result. Check the start and end time provided")
            return None
        else:
            df = pd.DataFrame(response["Items"])
            df = df.sort_values(by="Timestamp", ignore_index=True)

            df = self.filter_by_location(df, location)
            df = self.remove_false_positives(df)
            df = self.remove_weekends(df)
            df = self.remove_after_office_hours(df)

            return df

        # except:
        #     print("You have not connected to AWS successfully. Check your internet connection and security details")


    def converting_user_input_to_unixtime(self, start_time, end_time):
        try:
            start_time = datetime.timestamp(datetime.strptime(start_time, "%Y-%m-%d"))
            end_time = datetime.timestamp(datetime.strptime(end_time, "%Y-%m-%d"))

            return start_time, end_time
        except:
            print("The date format provided does not match format 'YYYY-mm-dd'")

    def filter_by_location(self, data, location):
        data = data[data["Location"] == location].reset_index(drop=True)
        return data

    def remove_false_positives(self, data):
        """
        Removes sporadic data collected by the sensors during the data collection period.
        """
        data["Timestamp"] = data["Timestamp"].astype("int64")
        time_difference = data["Timestamp"].to_numpy()[1:] - data["Timestamp"].to_numpy()[:-1]
        time_difference = np.append(time_difference, time_difference[-1])
        data["t_diff"] = time_difference

        index_to_drop = []
        temp_list_1 = []  # used to store index of rows that indicates user presence but we are not sure
        temp_list_2 = []  # used to store index of rows that indicates user absence but we are not sure
        MAX_ERROR_COUNT = 3  # number of readings before user determined to be away from room
        success_count = 0
        error_count = 0
        for i in range(len(data)):
            if data.loc[i, "t_diff"] < 60:  # user is in the room
                success_count += 1
                error_count = 0
                temp_list_1.append(i)
                temp_list_2 = []
            else:
                error_count += 1

                if error_count == 1 and success_count == 1:  # capture cases where there are sporadic readings that happen close to each other
                    temp_list_2.extend(temp_list_1)
                    temp_list_2.append(i)
                elif error_count == 1:
                    continue
                elif error_count < MAX_ERROR_COUNT:  # possible that user is still in the room
                    temp_list_2.append(i)
                elif error_count == MAX_ERROR_COUNT:  # user first confirmed to be away from room for a certain period
                    temp_list_2.append(i)
                    index_to_drop.extend(temp_list_2)
                    temp_list_2 = []
                else:  # user is away from room
                    index_to_drop.append(i)

                success_count = 0
                temp_list_1 = []

        # print(index_to_drop)
        # data["drop"] = 0
        # data.loc[index_to_drop, 'drop'] = 1
        data = data.drop(index=index_to_drop, columns="t_diff").reset_index(drop=True)

        return data


    def remove_weekends(self, data):
        """
        Removes data collected during the weekends.
        """
        data["Timestamp"] = data["Timestamp"].astype("int64")
        timestamp_new = pd.to_datetime(data["Timestamp"], unit="s", utc=True)
        timestamp_new = timestamp_new.dt.tz_convert(tz='Asia/Singapore')
        data["Day_of_week"] = timestamp_new.dt.dayofweek
        data = data[data["Day_of_week"] < 5].reset_index(drop=True)
        data.drop(columns="Day_of_week", inplace=True)

        return data

    def remove_after_office_hours(self, data):
        """
        Removes the after office hours. Potential office hours defined: 07:00-20:00
        """
        data["Timestamp"] = data["Timestamp"].astype("int64")
        timestamp_new = pd.to_datetime(data["Timestamp"], unit="s", utc=True)
        timestamp_new = timestamp_new.dt.tz_convert(tz='Asia/Singapore')
        data["hour"] = timestamp_new.dt.hour

        working_hours = list(range(7, 20))
        data = data[data["hour"].isin(working_hours)].reset_index(drop=True)
        data.drop(columns="hour", inplace=True)

        return data


if __name__ == '__main__':
    aws = GetDataAWS()

    # Provide the start date, end date, location and Table Name (either DistanceData or MotionData)
    df = aws.extract_data("2021-10-05", "2021-11-7", "Adrian_Office", "MotionData")
    print(df)
    df.to_csv('Data.csv', index=False)



