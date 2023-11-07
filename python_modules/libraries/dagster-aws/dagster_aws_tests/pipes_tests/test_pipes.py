import base64
import json

import pytest
from dagster import asset, materialize, open_pipes_session
from dagster._core.pipes.utils import PipesEnvContextInjector
from dagster_aws.pipes import PipesLambdaClient, PipesLambdaLogsMessageReader

from .fake_lambda import LOG_TAIL_LIMIT, FakeLambdaClient, LambdaFunctions


def test_fake_lambda_logs():
    event = {}
    response = FakeLambdaClient().invoke(
        FunctionName=LambdaFunctions.trunc_logs.__name__,
        InvocationType="RequestResponse",
        Payload=json.dumps(event),
        LogType="Tail",
    )

    log_result = base64.b64decode(response["LogResult"])
    assert len(log_result) == LOG_TAIL_LIMIT

    small_size = 512
    response = FakeLambdaClient().invoke(
        FunctionName=LambdaFunctions.small_logs.__name__,
        InvocationType="RequestResponse",
        Payload=json.dumps({"size": small_size}),
        LogType="Tail",
    )

    log_result = base64.b64decode(response["LogResult"])
    assert len(log_result) == small_size + 1  # size + \n


def test_manual_fake_lambda_pipes():
    @asset
    def fake_lambda_asset(context):
        context_injector = PipesEnvContextInjector()
        message_reader = PipesLambdaLogsMessageReader()

        with open_pipes_session(
            context=context,
            message_reader=message_reader,
            context_injector=context_injector,
        ) as session:
            user_event = {}
            response = FakeLambdaClient().invoke(
                FunctionName=LambdaFunctions.pipes_basic.__name__,
                InvocationType="RequestResponse",
                Payload=json.dumps(
                    {
                        **user_event,
                        **session.get_bootstrap_env_vars(),
                    }
                ),
                LogType="Tail",
            )
            message_reader.consume_lambda_logs(response)
            yield from session.get_results()

    result = materialize([fake_lambda_asset])
    assert result.success
    mat_evts = result.get_asset_materialization_events()
    assert len(mat_evts) == 1
    assert mat_evts[0].materialization.metadata["meta"].value == "data"


def test_fake_client_lambda_pipes():
    @asset
    def fake_lambda_asset(context):
        return (
            PipesLambdaClient(FakeLambdaClient())
            .run(
                context=context,
                function_name=LambdaFunctions.pipes_basic.__name__,
                event={},
            )
            .get_materialize_result()
        )

    result = materialize([fake_lambda_asset])
    assert result.success
    mat_evts = result.get_asset_materialization_events()
    assert len(mat_evts) == 1
    assert mat_evts[0].materialization.metadata["meta"].value == "data"


def test_fake_client_lambda_error():
    @asset
    def fake_lambda_asset(context):
        yield from (
            PipesLambdaClient(FakeLambdaClient())
            .run(
                context=context,
                function_name=LambdaFunctions.error.__name__,
                event={},
            )
            .get_results()
        )

    with pytest.raises(Exception, match="Lambda Function Error"):
        materialize([fake_lambda_asset])
