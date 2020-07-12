import { S3Event, Context } from "aws-lambda";
import * as AWS from "aws-sdk";

const s3 = new AWS.S3();
const db = new AWS.DynamoDB.DocumentClient();

let addQuizToDB = async (fileDate: string, data: AWS.S3.GetObjectOutput) => {
    if(!data.Body) return;

    let contents = JSON.parse(data.Body.toString());

    let params = {
        TableName: process.env.TABLE_NAME ?? "dev-quiz-questions",
        Item: {
            quiz_date: fileDate,
            questions: contents["questions"]
        },
    };

    await db.put(params).promise().catch(err => console.log(`Error putting quiz questions into DB: ${err}`))
};

export const storeQuiz = async (event: S3Event, context: Context) => {
    console.log(event);
    const srcBucket = event.Records[0].s3.bucket.name;
    const srcKey = decodeURIComponent(
        event.Records[0].s3.object.key.replace(/\+/g, " ")
    );

    const fileParts = srcKey.match("quiz_([^.]*).json");

    if (!fileParts) {
        console.log(`Could not determine quiz date, key was: ${srcKey}`);
        return;
    }

    const fileDate = fileParts[1];

    try {
        const params = {
            Bucket: srcBucket,
            Key: srcKey,
        };

        await s3
            .getObject(params)
            .promise()
            .then((data) => addQuizToDB(fileDate, data));
    } catch (error) {
        console.log(error);
        context.captureError(error);
        return;
    }

    return;
};
