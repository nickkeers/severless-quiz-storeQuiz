import { S3Event, Context, Callback } from "aws-lambda";
import * as AWS from "aws-sdk";
import "serverless";

const s3 = new AWS.S3();
const db = new AWS.DynamoDB.DocumentClient();

let addQuizToDB = async (fileDate: string, data: AWS.S3.GetObjectOutput) => {
    if(!data.Body) throw "Empty S3 questions body";

    let contents = JSON.parse(data.Body.toString());

    let params = {
        TableName: process.env.TABLE_NAME ?? "dev-quiz-questions",
        Item: {
            quiz_date: fileDate,
            questions: contents["questions"]
        },
    };

    await db.put(params).promise().catch(err => console.error(`Error putting quiz questions into DB: ${err}`))
};

export const storeQuiz = async (event: S3Event, context: Context, callback: Callback) => {
    console.log(event);
    const srcBucket = event.Records[0].s3.bucket.name;
    const srcKey = decodeURIComponent(
        event.Records[0].s3.object.key.replace(/\+/g, " ")
    );

    const fileParts = srcKey.match("quiz_([^.]*).json");

    if (!fileParts) {
        console.error(`Could not determine quiz date, key was: ${srcKey}`);
        return callback(`Could not determine quiz date, key was: ${srcKey}`, {});
    }

    const fileDate = fileParts[1];

    try {
        const params = {
            Bucket: srcBucket,
            Key: srcKey,
        };

        return s3
            .getObject(params)
            .promise()
            .then((data) => addQuizToDB(fileDate, data));
    } catch (error) {
        console.error(error);
        return callback(error, {});
    }
};
