git checkout main
git rebase dev

serverless deploy --stage production

git push
git checkout dev
