create sequence replica.seq_servers;
create sequence replica.seq_include_rules;
create sequence replica.seq_exclude_rules;
create sequence replica.seq_data;
create sequence replica.seq_answers;
create sequence replica.seq_issues;
create type replica.issue as enum('connection_timeout', 'connection_rejected', 'send_timeout', 'waiting_closed', 'wrong_answer');
create type replica.connection_types as enum('soft', 'aggressive');

create table replica.servers(
  id bigint,
  owner_id bigint not null,
  hostname varchar not null,
  port bigint not null,
  protocols terminals.protocols[] not null,
  max_points bigint not null default 1,
  retry_interval interval not null default '00:00:01',
  max_connections bigint,
  connection_type replica.connection_types not null default 'soft',

  constraint zidx_servers_pk primary key(id),
  constraint zidx_servers_fk_owner foreign key(owner_id) references owners.data(id) on delete cascade
);

create table replica.include_rules(
  id bigint,
  server_id bigint not null,
  object_id bigint,
  group_id bigint,
  specialization_id bigint,
  local_port bigint,
  terminal_protocol terminals.protocols,

  constraint zidx_include_rules_pk primary key(id),
  constraint zidx_include_rules_fk_server foreign key(server_id) references replica.servers(id) on delete cascade,
  constraint zidx_include_rules_fk_object foreign key(object_id) references objects.data(id) on delete cascade,
  constraint zidx_include_rules_fk_group foreign key(group_id) references objects.groups(id) on delete cascade,
  constraint zidx_include_rules_check check ((
      (object_id is not null)::integer +
      (group_id is not null)::integer +
      (specialization_id is not null)::integer +
      (local_port is not null)::integer +
      (terminal_protocol is not null)::integer)
    = 1)
);

create table replica.exclude_rules(
  id bigint,
  server_id bigint not null,
  object_id bigint,
  group_id bigint,
  specialization_id bigint,
  local_port bigint,
  terminal_protocol terminals.protocols,

  constraint zidx_exclude_rules_pk primary key(id),
  constraint zidx_exclude_rules_fk_server foreign key(server_id) references replica.servers(id) on delete cascade,
  constraint zidx_exclude_rules_fk_object foreign key(object_id) references objects.data(id) on delete cascade,
  constraint zidx_exclude_rules_fk_group foreign key(group_id) references objects.groups(id) on delete cascade,
  constraint zidx_exclude_rules_check check ((
      (object_id is not null)::integer +
      (group_id is not null)::integer +
      (specialization_id is not null)::integer +
      (local_port is not null)::integer +
      (terminal_protocol is not null)::integer)
    = 1)
);

create table replica.data(
  id bigint,
  dbtime timestamp with time zone default now(),
  parent_id bigint not null,
  server_id bigint not null,
  protocol terminals.protocols not null,
  terminal_id bigint not null,
  data bytea not null,
  answer_id bigint,

  constraint zidx_data_pk primary key(id),
  constraint zidx_data_fk_parent foreign key(parent_id) references data.packets(id) on delete cascade,
  constraint zidx_data_fk_server foreign key(server_id) references replica.servers(id) on delete cascade
);
create index zidx_data_ik_dbtime_server on replica.data(dbtime, server_id) where answer_id is null;
create index zidx_data_ik_dbtime_server_terminal on replica.data(dbtime, server_id, terminal_id) where answer_id is null;
create index zidx_data_ik_answer on replica.data(answer_id);
create index zidx_data_ik_parent on replica.data(parent_id);

create table replica.answers(
  id bigint,
  dbtime timestamp with time zone default now(),
  connection_id bigint not null,
  data bytea not null,

  constraint zidx_answers_pk primary key(id),
  constraint zidx_answers_fk_connection foreign key(connection_id) references data.connections(id) on delete cascade
);
create index zidx_answers_ik_connection on replica.answers(connection_id);
alter table replica.data add constraint zidx_data_fk_answer foreign key(answer_id) references replica.answers(id) on delete cascade;

create table replica.issues(
  id bigint,
  dbtime timestamp with time zone default now(),
  server_id bigint not null,
  packets_ids bigint[] not null,
  connection_id bigint not null,
  issue replica.issue not null,

  constraint zidx_issues_pk primary key(id),
  constraint zidx_issues_fk_server foreign key(server_id) references replica.servers(id) on delete cascade,
  constraint zidx_issues_fk_connection foreign key(connection_id) references data.connections(id) on delete cascade
);
create index zidx_answer_ik_connection_dbtime on replica.issues(connection_id, dbtime);

create trigger insertb_00_reject
  before insert on replica.data for each row
  when (new.id is not null)
  execute procedure triggers.reject();

create trigger insertb_00_set_id
  before insert on replica.data for each row
  when (new.id is null)
  execute procedure triggers.set_id();

create rule add_answer as on insert to replica.data where (new.id is not null)
  do also
    update replica.data
    set answer_id=new.answer_id
    where id=new.id;

create trigger insertb_00_set_id
  before insert on replica.servers for each row
  when (new.id is null)
  execute procedure triggers.set_id();

create trigger insertb_00_set_id
  before insert on replica.include_rules for each row
  when (new.id is null)
  execute procedure triggers.set_id();

create trigger insertb_00_set_id
  before insert on replica.exclude_rules for each row
  when (new.id is null)
  execute procedure triggers.set_id();

create trigger insertb_00_set_id
  before insert on replica.answers for each row
  when (new.id is null)
  execute procedure triggers.set_id();


create trigger insertb_00_set_id
  before insert on replica.issues for each row
  when (new.id is null)
  execute procedure triggers.set_id();
