from prefect.filesystems import GitHub

github_storage_block = GitHub(
    repository="https://github.com/geraldooi/de-zoomcamp.git",
    reference="main"
)

github_storage_block.save("de-zoomcamp", overwrite=True)