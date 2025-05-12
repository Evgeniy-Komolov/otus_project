def get_conn_creds(value: dict, get_all_key: bool = False) -> dict:
    """
    Функция, которая получает пароль из connections

    Args:
        value: macros из AF
        get_all_key: флаг определяющий необходимость вывести ВСЕ ключ-значения  макроса

    Returns:
        dict - ключ-значения из макроса
    """
    # import json
    if get_all_key:
        return value
    else:
        return dict(login=value['login'], password=value['password'])