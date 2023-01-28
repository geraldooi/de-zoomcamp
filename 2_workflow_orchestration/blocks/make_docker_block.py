from prefect.infrastructure.docker import DockerContainer

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="geraldooi/prefect:de-zoomcamp",
    image_pull_policy="ALWAYS",
    auto_remove=True,
    network_mode="bridge"
)

docker_block.save("prefect", overwrite=True)