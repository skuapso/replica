create function replica.servers(_connection_id bigint) returns table(
  server_id bigint,
  server_protocol terminals.protocols) as $$
declare
  _terminal_id bigint;
begin
  _terminal_id = connection.terminal(_connection_id);
  return query select * from replica.servers(_terminal_id, connection_id);
end $$ language plpgsql;

create or replace function replica.servers(_terminal_id bigint, _connection_id bigint) returns table(
  server_id bigint,
  server_protocol terminals.protocols) as $$
declare
  _object_id bigint;
  _group_id bigint;
  _specialization_id bigint;
  _local_port bigint;
  _terminal_protocol terminals.protocols;
begin
  _object_id = object.get(_terminal_id);
  _group_id = object.group(_object_id);
  _specialization_id = object.specialization(_object_id);
  _local_port = connection.local_port(_connection_id);
  _terminal_protocol = connection.protocol(_connection_id);
  return query
    select * from
    replica.servers(_object_id, _group_id, _specialization_id, _local_port, _terminal_protocol);
end $$ language plpgsql;

create function replica.servers(
  _object_id bigint,
  _group_id bigint,
  _specialization_id bigint,
  _local_port bigint,
  _terminal_protocol terminals.protocols)
returns table(
  server_id bigint,
  server_protocol terminals.protocols) as $$
begin
  return query select distinct S.server_id,replica.server_protocol(S.server_id, _terminal_protocol) from (
    select IR1.server_id
    from replica.include_rules IR1
    where terminal_protocol = _terminal_protocol
    and IR1.server_id not in (
      select ER1.server_id
      from replica.exclude_rules ER1
      where (local_port = _local_port
        or object_id=_object_id
        or specialization_id=_specialization_id
        or array[_group_id] <@ ("group".childs(group_id) || group_id)
        or local_port=_local_port))

    union
    select IR2.server_id
    from replica.include_rules IR2
    where local_port = _local_port
    and IR2.server_id not in (
      select ER2.server_id
      from replica.exclude_rules ER2
      where (terminal_protocol = _terminal_protocol
        or object_id=_object_id
        or specialization_id = _specialization_id
        or array[_group_id] <@ ("group".childs(group_id) || group_id)))

    union
    select IR3.server_id
    from replica.include_rules IR3
    where array[_group_id] <@ ("group".childs(group_id) || group_id)
    and IR3.server_id not in (
      select ER3.server_id
      from replica.exclude_rules ER3
      where (terminal_protocol=_terminal_protocol
        or local_port=_local_port
        or object_id=_object_id
        or specialization_id = _specialization_id
        or array[_group_id] <@ ("group".childs(group_id) || group_id)))

    union
    select IR4.server_id
    from replica.include_rules IR4
    where specialization_id=_specialization_id
    and IR4.server_id not in (
      select ER4.server_id
      from replica.exclude_rules ER4
      where object_id=_object_id)

    union
    select IR5.server_id
    from replica.include_rules IR5
    where object_id = _object_id
  ) S;
end $$ language plpgsql stable;

create function replica.server_protocol(_id bigint, _prefered terminals.protocols) returns terminals.protocols as $$
declare
  p terminals.protocols;
begin
  select $2 into p from replica.servers where id=$1 and array[$2] <@ protocols;
  if found then
    return p;
  end if;
  select protocols[1] into p from replica.servers where id=$1;
  return p;
end $$ language plpgsql stable;

create function replica.data() returns table(
  server_id bigint,
  protocol terminals.protocols,
  terminal_protocol terminals.protocols,
  terminal_uin bigint) as $$
begin
  return query select S.server_id,S.protocol, S.tproto[1], S.tuin
    from (
      select D.server_id,D.protocol,terminal.protocols(D.terminal_id) as tproto,terminal.uin(D.terminal_id) as tuin
      from replica.data D
      where answer_id is null
      order by dbtime limit 1) as S;
end $$ language plpgsql;

create function replica.data(_server_id bigint) returns table(
  server_id bigint,
  protocol terminals.protocols,
  terminal_protocol terminals.protocols,
  terminal_uin bigint) as $$
begin
  return query select S.server_id,S.protocol, S.tproto[1], S.tuin
    from (
      select D.server_id,D.protocol,terminal.protocols(D.terminal_id) as tproto,terminal.uin(D.terminal_id) as tuin
      from replica.data D
      where answer_id is null and D.server_id=$1
      order by dbtime limit 1) as S;
end $$ language plpgsql;
