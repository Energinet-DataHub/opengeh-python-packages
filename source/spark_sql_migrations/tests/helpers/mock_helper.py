def base_path_helper(storage_account: str, container: str, folder: str = "") -> str:
    return container


def tables_path_helper(
    storage_account: str,
    container: str,
    table_name: str = "",
    folder: str = "",
    table_folder_name: str = "",
) -> str:
    return container


def do_nothing() -> None:
    pass
