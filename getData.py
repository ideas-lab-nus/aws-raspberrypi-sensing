import boto3
import pandas as pd
from datetime import datetime
from decimal import Decimal


from boto3.dynamodb.conditions import Key, Attr
dynamodb = boto3.resource('dynamodb')


class GetDataAWS:
    """
    This class extracts distance and motion data from AWS by filtering specific start & end date and table name.
    """

    def extract_data(self, start_time, end_time, table_name):

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


if __name__ == '__main__':
    aws = GetDataAWS()

    # Provide the start date, end date and Table Name (either DistanceData or MotionData)
    df = aws.extract_data("2021-10-05", "2021-10-06", "DistanceData")
    print(df)




