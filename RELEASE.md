# Release process (for Datadog employees only)

1. Update the [version](https://github.com/DataDog/sketches-java/blob/master/build.gradle#L17)
2. Go to [Gitlab](https://gitlab.ddbuild.io/DataDog/sketches-java/-/pipelines) and trigger the `deploy_to_sonatype` job
3. If it fails because the key expired, trigger the `create_key` job, then trigger `deploy_to_sonatype` job again
