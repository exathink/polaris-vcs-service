#!/usr/bin/env bash
set -e
export DONT_PROMPT_FOR_CONFIRMATION=1
echo "Deploying Image"
package aws deploy-image

echo "Deploying Task Definitions.."
package aws deploy-task-definition polaris-vcs-db-migrator
package aws deploy-task-definition polaris-vcs-service
package aws deploy-task-definition polaris-vcs-listener

echo "Running migrations"
# package aws run-task polaris-vcs-db-migrator

echo "Deploying Services.."
package aws deploy-services polaris-vcs-service polaris.auto-scaling-group   polaris-vcs-listener polaris-vcs-service
