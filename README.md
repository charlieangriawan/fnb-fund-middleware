# Staffinc 3.0 Chat

This project is a serverless application built using NodeJS. It is designed to run on AWS Lambda and is structured to facilitate easy development and deployment.

## Setup Instructions

1. **Install dependencies:**
   ```
   npm install
   ```

2. **Setup the domain on AWS:**
   ```
   sls create_domain --stage production
   ```

3. **Deploy the application:**
   ```
   sls deploy --stage production
   ```

## Usage

To run locally, use the following command:
```
npm run dev
```

You can invoke handlers the using the following format:
```
sls invoke local -f healthCheck
```
