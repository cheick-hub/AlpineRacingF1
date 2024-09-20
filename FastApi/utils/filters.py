from sqlalchemy import or_


def filter_eq(q, column, values_list):
    if values_list is None or values_list == []:
        return q
    # filter_list = []
    if type(values_list) != type([]):
        values_list = [values_list]
    # for value in values_list:
    #     filter_list.append(column == value)
    # if filter_list:
    #     q = q.filter(or_(*filter_list))
    q = q.filter(column.in_(values_list))
    return q


def filter_like(q, column, values_list):
    if values_list is None or values_list == []:
        return q
    if type(values_list) != type([]):
        values_list = [values_list]
    filter_list = []
    for value in values_list:
        filter_list.append(column.like(value))
    if filter_list:
        q = q.filter(or_(*filter_list))
    return q
