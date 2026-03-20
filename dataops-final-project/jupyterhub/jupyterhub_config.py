from datetime import datetime, timedelta
from typing import Any, Tuple

from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from dateutil.relativedelta import relativedelta

from operators.crossweb.yt_common import (
    YT_ODS_DB_PATH,
    export_holding_dict,
    export_project_dict,
    make_and_push_json,
    set_reload_ids,
)
from operators.yt_crossweb_operators import (
    CrosswebDictOperator,
    CrosswebOperator,
)

CONN_ID = "ytsaurus_dataoffice"


def generate_spyt_submit_command(
        *,
        jars_home,
        application_args,
        meta_kwargs=None,
):
    """Generate bash command for submitting Spark job to YTsaurus.

    Args:
        jars_home: Path to JAR file
        application_args: Dict of application arguments
        meta_kwargs: Dict of additional spark-submit arguments

    Returns:
        str: Complete bash command
    """
    # Convert application args dict to command line format
    app_args_list = []
    for key, value in application_args.items():
        if isinstance(value, list):
            app_args_list.extend([f"--{key}", " ".join(str(v) for v in value)])
        else:
            if value != "":
                app_args_list.extend([f"--{key}", str(value)])
    application_args_str = " ".join(app_args_list)

    # Convert meta kwargs to command line format
    meta_kwargs = meta_kwargs or {}
    meta_kwargs_str = " ".join([f"--{key} {value}" for key, value in meta_kwargs.items()])

    return f"""
        source /usr/local/ytsaurus/bin/activate
        source spyt-env
        export YT_TOKEN={{{{ conn.{CONN_ID}.password }}}}
        export YT_USER={{{{ conn.{CONN_ID}.login }}}}
        export YT_PROXY=jupiter.yt.vk.team
        export YT_REMOTE_PROXY=http-proxy.jupiter-yt.hc.one-infra.ru
        log=$(mktemp)
        spark-submit --master ytsaurus://$YT_PROXY --deploy-mode cluster \
        --queue dataoffice-prod-spyt \
        --conf spark.hadoop.yt.clusterProxy=$YT_REMOTE_PROXY \
        --conf spark.hadoop.yt.clusterProxyRole=hc \
        --conf spark.ui.reverseProxy=true  \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.ytsaurus.redirect.stdout.to.stderr=true \
        --conf spark.yt.read.typeV3.enabled=true \
        --conf spark.yt.write.typeV3.enabled=true \
        --conf spark.hadoop.yt.write.typeV3.enabled=true \
        --conf spark.yarn.executor.memoryOverhead=5Gb \
        --conf spark.shuffle.service.enabled=true  \
        --conf spark.shuffle.service.port=7337  \
        --conf spark.local.dir=/yt/shuffle \
        --conf spark.executorEnv.YT_SPARK_LOCAL_DIRS=/yt/shuffle \
        --conf spark.shuffle.useOldFetchProtocol=true \
        --conf spark.excludeOnFailure.killExcludedExecutors=true \
        --conf spark.yt.schema.forcingNullableIfNoMetadata.enabled=false \
        --conf spark.ytsaurus.driver.operation.parameters='{{scheduling_tag_filter="shuffle_service";pool_trees=[default_hc];acl=[{{"action"="allow"; "subjects"=["dataoffice_data_engineers"; "a.shishkov"]; "permissions"=["read"; "manage"]}}];}}' \
        --conf spark.ytsaurus.executor.operation.parameters='{{scheduling_tag_filter="shuffle_service";pool_trees=[default_hc];acl=[{{"action"="allow"; "subjects"=["dataoffice_data_engineers"; "a.shishkov"]; "permissions"=["read"; "manage"]}}];}}' \
        --verbose \
        {meta_kwargs_str} \
        {jars_home} \
        {application_args_str} 2>&1 | tee "$log"
        log_contents=$(cat "$log")
        operation_id=$(echo "$log_contents" | grep -oP 'Driver operation ID: \K[\w-]+')
        json_data=$(yt list-jobs "$operation_id")
        state=$(echo "$json_data" | grep -oP '(?<="state" = ")[^"]+' | head -n 1)
        if [ "$state" = "completed" ]; then
            echo "Job '$state'"
        else
            first_job_id=$(echo "$json_data" | grep -oP '(?<="id" = ")[^"]+' | head -n 1)
            echo "Driver's error log:"
            yt get-job-stderr $first_job_id $operation_id >&2
            exit 1
        fi
    """


def generate_dict_pipeline(
        source_path, target_path, today_date, retries, dag, sensor_interval, sensor_timeout, sensor_retries
) -> EmptyOperator:
    sensor_dag_start = EmptyOperator(task_id="sensor_dag_start", retries=retries)
    # ----------------------------------------------------------------------------------------------------------------------
    # Demo dict
    export_demo_dict = CrosswebDictOperator(
        task_id="export_demo_dict",
        dict_name="demo",
        yt_path=f"{target_path}/cw_demo_dict/{today_date}",
        retries=retries,
    )
    # ----------------------------------------------------------------------------------------------------------------------
    # Media dict
    export_media_dict = CrosswebDictOperator(
        task_id="export_media_dict",
        dict_name="media",
        yt_path=f"{target_path}/cw_media_dict/{today_date}",
        retries=retries,
    )

    # ----------------------------------------------------------------------------------------------------------------------
    # Google dicts
    # hdfs_google_spreadsheet_sensor_task = HdfsSensor(
    #     task_id="hdfs_google_spreadsheet_sensor_task",
    #     filepath=f"{source_path}/{today_date}/_SUCCESS",
    #     timeout=sensor_timeout,
    #     mode="reschedule",
    #     poke_interval=sensor_interval,
    #     soft_fail=False,
    #     retries=sensor_retries,
    # )

    make_holdings_json = PythonOperator(
        task_id="make_holdings_json",
        python_callable=make_and_push_json,
        op_kwargs={"today_date": today_date},
        provide_context=True,
        dag=dag,
        retries=retries,
    )

    set_reload_by_project_var = PythonOperator(
        task_id="set_reload_by_project_var",
        python_callable=set_reload_ids,
        op_kwargs={"today_date": today_date},
        dag=dag,
        retries=retries,
    )

    exp_project_dict = PythonOperator(
        task_id="export_project_dict",
        python_callable=export_project_dict,
        op_kwargs={"today_date": today_date},
        dag=dag,
        retries=retries,
    )

    exp_holding_dict = PythonOperator(
        task_id="export_holding_dict",
        python_callable=export_holding_dict,
        op_kwargs={"today_date": today_date},
        dag=dag,
        retries=retries,
    )

    eod_dict = EmptyOperator(task_id="eod_dict", retries=retries)

    sensor_dag_start >> [
        export_media_dict,
        export_demo_dict,
    ]

    (sensor_dag_start >> make_holdings_json >> set_reload_by_project_var,)

    (sensor_dag_start >> exp_project_dict,)

    (sensor_dag_start >> exp_holding_dict,)

    eod_dict << [export_media_dict, export_demo_dict, set_reload_by_project_var, exp_project_dict, exp_holding_dict]
    eod_dict << export_media_dict
    return eod_dict


def generate_mobstore_dict_pipeline(
        source_path, target_path, today_date, retries, dag, sensor_interval, sensor_timeout, sensor_retries
) -> EmptyOperator:
    sensor_dag_start = EmptyOperator(task_id="sensor_dag_start", retries=retries)

    make_holdings_json = PythonOperator(
        task_id="make_holdings_json",
        python_callable=make_and_push_json,
        op_kwargs={"today_date": today_date, "project": "mobstore"},
        provide_context=True,
        dag=dag,
        retries=retries,
    )

    set_reload_by_project_var = PythonOperator(
        task_id="set_reload_by_project_var",
        python_callable=set_reload_ids,
        op_kwargs={"today_date": today_date, "project": "mobstore"},
        dag=dag,
        retries=retries,
    )

    exp_project_dict = PythonOperator(
        task_id="export_project_dict",
        python_callable=export_project_dict,
        op_kwargs={"today_date": today_date,
                   "project_dict_name": "mobstore_project_dict.tsv"},
        dag=dag,
        retries=retries,
    )

    exp_holding_dict = PythonOperator(
        task_id="export_holding_dict",
        python_callable=export_holding_dict,
        op_kwargs={"today_date": today_date,
                   "holding_dict_name": "mobstore_holding_dict.tsv"},
        dag=dag,
        retries=retries,
    )

    eod_dict = EmptyOperator(task_id="eod_dict", retries=retries)

    (sensor_dag_start >> make_holdings_json >> set_reload_by_project_var,)

    (sensor_dag_start >> exp_project_dict,)

    (sensor_dag_start >> exp_holding_dict,)

    eod_dict << [set_reload_by_project_var, exp_project_dict, exp_holding_dict]

    return eod_dict


def generate_pipe(
        start_task,
        reload,
        partition,
        start_load_dt,
        end_load_dt,
        retries,
        today_date,
        target_path,
        statistics,
        job_date,
        job_defaults,
        load_type,
        partition_type,
        partition_path,
        date_type,
        holding_list,
        all_mart_filter: Any = None,
        is_mobstore_task: bool = False,
        id_to_reload="",
        demo: Tuple = ("cityPop", "ageGroup", "federalOkrug"),
        dinamic=False,
) -> BaseOperator:
    def split_date_range(start_date_str, end_date_str):
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        current_start = start_date
        intervals = []

        while current_start < end_date:
            # Определяем конец текущего месяца
            next_month_start = (current_start + relativedelta(months=1)).replace(day=1)
            current_end = min(next_month_start - timedelta(days=1), end_date)

            intervals.append((current_start.strftime("%Y_%m_%d"), current_end.strftime("%Y_%m_%d")))
            current_start = next_month_start

        return intervals

    is_holding_task = False
    if load_type == "holding" or load_type == "holding_no_demo":
        is_holding_task = True

    demo_name = ""
    if len(demo) == 1:
        demo_name = demo[0]

    dir_name = "_" + load_type
    if load_type == "simple":
        dir_name = ""
    if load_type == "simple_no_demo":
        dir_name = "_no_demo"
    full_target_path = (
        f"{YT_ODS_DB_PATH}/cw{dir_name}_audience{partition_path}_full/cw{dir_name}_audience{partition_path}_full"
    )

    by_project = reload["by_project"]
    is_reload = reload["is_reload"]

    if dinamic:
        intervals = split_date_range(str(start_load_dt), str(end_load_dt))
    else:
        intervals = [(start_load_dt, end_load_dt)]
    first_task = start_task
    for start, end in intervals:
        if dinamic:
            load_task_id = f"{load_type}_{partition_type}_{start}_{end}"
            load_name = f"cw{dir_name}_audience_{partition_type}{by_project}_{start}_{end}"
            merge_task_id = f"merge_{load_type}_{partition_type}_{start}_{end}"
        else:
            load_task_id = f"{load_type}_{partition_type}"
            load_name = f"cw{dir_name}_audience_{partition_type}{by_project}"
            merge_task_id = f"spyt_merge_{load_type}_{partition_type}"
        MSCOPE_SPYT_CONFIG = {
            "jars_home": "yt:///home/dataoffice/z_maintenance/artifacts/YTJobs.jar",
            "meta_kwargs": {
                "driver-memory": "6g",
                "executor-memory": "16g",
                "executor-cores": "4",
                "num-executors": "20",
                "class": "com.dataoffice.mediascope.StageMscopeMerge",
            },
            "application_args": {
                "delta_yt": f"yt:/{target_path}/cw{dir_name}_audience{partition_path}{by_project}/1d/{partition}/csv/{load_name}.csv.gz",
                "source_name": f"cw{dir_name}_audience{partition_path}",
                "start_date": str(start),
                "end_date": str(end),
                "full_yt": full_target_path,
                "is_reload": str(is_reload),
                "id_to_reload": id_to_reload,
                "today_date": str(today_date),
                "temp_yt": f"{target_path}/tmp/cw{dir_name}_audience{partition_path}{by_project}/{partition}/{partition}",
                "demo_name": str(demo_name),
            },
        }
        yt_path = f"{target_path}/cw{dir_name}_audience{partition_path}{by_project}"
        if is_mobstore_task:
            yt_path = f"{target_path}/mobstore{dir_name}_audience{partition_path}{by_project}"
            MSCOPE_SPYT_CONFIG['application_args'] = {
                "delta_yt": f"yt:/{target_path}/mobstore{dir_name}_audience{partition_path}{by_project}/1d/{partition}/csv/{load_name}.csv.gz",
                "source_name": f"cw{dir_name}_audience{partition_path}",
                "start_date": str(start),
                "end_date": str(end),
                "full_yt": full_target_path.replace('/cw', '/mobstore'),
                "is_reload": str(is_reload),
                "id_to_reload": id_to_reload,
                "today_date": str(today_date),
                "temp_yt": f"{target_path}/tmp/mobstore{dir_name}_audience{partition_path}{by_project}/{partition}/{partition}",
                "demo_name": str(demo_name),
            }

        load = CrosswebOperator(
            task_id=load_task_id,
            name=load_name,
            partition=partition,
            yt_path=yt_path,
            load_start_date=start,
            load_end_date=end,
            all_mart_filter=all_mart_filter,
            is_holding_task=is_holding_task,
            is_mobstore_task=is_mobstore_task,
            is_hist_task=reload["reload_flag"],
            holding_list=holding_list,
            date_type=date_type,
            statistic=statistics,
            retries=retries,
            demo=demo,
            pool="crossweb_ms_pool",
        )
        first_task >> load

        merge = BashOperator(
            task_id=merge_task_id,
            execution_timeout=timedelta(hours=24),
            bash_command=generate_spyt_submit_command(**MSCOPE_SPYT_CONFIG),
            retries=retries,
        )
        load >> merge
        first_task = merge

    unlock_merge = EmptyOperator(
        task_id=f"unlock_{load_type}_{partition_type}",
        retries=retries,
    )
    merge >> unlock_merge
    return unlock_merge


def generate_start(
        pre_task,
        pipe,
        partition,
        task,
        retries,
        push_function: Any = None,
        start_load_dt: Any = None,
        is_daily: Any = None,
        today: Any = None,
        is_mobstore_task: bool = False
) -> BaseOperator:
    start = EmptyOperator(task_id=f"{pipe}_{partition}_{task}", retries=retries)
    last_task = start
    pre_task >> start
    if push_function is not None:
        push = PythonOperator(
            task_id=f"push_{pipe}_{partition}",
            python_callable=push_function,
            op_kwargs={"target_date": start_load_dt,
                       "is_daily": is_daily,
                       "today_date": today,
                       "is_mobstore_task": is_mobstore_task},
            provide_context=True,
            retries=retries,
        )
        last_task = push
        start >> push
    return last_task


def generate_crossweb(
        pre_tasks, today_date, generate_options_p, retries, target_path, job_date, job_defaults
) -> BaseOperator:
    partitions = generate_options_p["partitions"]
    demo_options = generate_options_p["demo_options"]

    for partition in partitions.keys():
        pipes = partitions[partition]["pipes"]
        for pipe in pipes.keys():
            for task in partitions[partition]["tasks"]:
                if task == "start":
                    pre_tasks[pipe] = generate_start(
                        pre_task=pre_tasks[pipe],
                        partition=partition,
                        pipe=pipe,
                        task=task,
                        push_function=pipes[pipe]["push_function"],
                        start_load_dt=partitions[partition]["start_load_dt"],
                        is_daily=partitions[partition]["is_daily"],
                        today=today_date,
                        retries=retries,
                        is_mobstore_task=pipes[pipe].get('is_mobstore_task', False)
                    )

                if task == "main":
                    last_main_task = []
                    for demo_name in demo_options.keys():
                        load_type = pipe
                        if demo_name == "no_demo":
                            load_type += "_" + demo_name

                        if partition == "daily":
                            partition_dates = partitions[partition]["start_load_dt"]
                        else:
                            partition_dates = (
                                    partitions[partition]["start_load_dt"] + "_" + partitions[partition]["end_load_dt"]
                            )
                        last_main_task.append(
                            generate_pipe(
                                start_task=pre_tasks[pipe],
                                partition=partition_dates,
                                start_load_dt=partitions[partition]["start_load_dt"],
                                end_load_dt=partitions[partition]["end_load_dt"],
                                dinamic=partitions[partition]["dinamic"],
                                retries=retries,
                                today_date=today_date,
                                target_path=target_path,
                                statistics=partitions[partition]["statistics"],
                                job_date=job_date,
                                job_defaults=job_defaults,
                                partition_type=partition,
                                partition_path=partitions[partition]["partition_path"],
                                load_type=load_type,
                                all_mart_filter=pipes[pipe]["all_mart_filter"],
                                is_mobstore_task=pipes[pipe].get('is_mobstore_task', False),
                                date_type=partitions[partition]["date_type"],
                                demo=demo_options[demo_name],
                                reload=partitions[partition]["reload"],
                                holding_list=pipes[pipe]["holding_list"],
                                id_to_reload=pipes[pipe]["id_to_reload"],
                            )
                        )
                    pre_tasks[pipe] = last_main_task

                if task == "end":
                    pre_tasks[pipe] = generate_start(
                        pre_task=pre_tasks[pipe],
                        partition=partition,
                        pipe=pipe,
                        task=task,
                        retries=retries,
                        push_function=None,
                        is_mobstore_task=pipes[pipe].get('is_mobstore_task', False),
                    )

    return pre_tasks
