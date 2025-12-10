# Runtime image for Flink job pods. The user jar is no longer embedded in the image.
# The jar will be built in CI and referenced remotely by the FlinkSessionJob via jarURI.
FROM flink:1.20.2

# No application jar is copied into the image anymore.
# Default command is provided by Flink image; the Operator will set the job parameters.
