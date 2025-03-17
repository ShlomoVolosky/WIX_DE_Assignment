import boto3

def list_polygon_flatfiles():
    # Replace these with your actual S3 credentials from the Polygon.io dashboard
    S3_ENDPOINT = "https://files.polygon.io"
    ACCESS_KEY_ID = "9f88f71a-3bc9-45f4-ae50-f953917e0261"
    SECRET_ACCESS_KEY = "bbjjN0sjtckiubsIzAIPeYISDPShAI3w"
    BUCKET_NAME = "flatfiles"

    # Create an S3 client pointing to Polygon’s endpoint
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY_ID,
        aws_secret_access_key=SECRET_ACCESS_KEY
    )

    try:
        # List all objects in the bucket (you can apply filters or pagination as needed)
        response = s3.list_objects_v2(Bucket=BUCKET_NAME)
        contents = response.get("Contents", [])

        print(f"Found {len(contents)} objects in bucket '{BUCKET_NAME}':\n")
        for obj in contents:
            print(obj["Key"])

    except Exception as e:
        print(f"An error occurred when accessing Polygon's S3: {e}")


if __name__ == "__main__":
    list_polygon_flatfiles()
