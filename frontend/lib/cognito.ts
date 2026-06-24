"use client";
import { Amplify } from "aws-amplify";

const userPoolId = process.env.NEXT_PUBLIC_COGNITO_USER_POOL_ID || "";
const userPoolClientId = process.env.NEXT_PUBLIC_COGNITO_CLIENT_ID || "";

if (userPoolId && userPoolClientId) {
  Amplify.configure({
    Auth: {
      Cognito: {
        userPoolId,
        userPoolClientId,
        loginWith: {
          email: true,
        },
      },
    },
  });
}

export { userPoolId, userPoolClientId };
