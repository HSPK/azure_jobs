from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from azure_jobs.server.az_client.arm.subscriptions import SubscriptionsAPI
from azure_jobs.server.submit.azureml.workspace import resolve_target
from azure_jobs.server.submit.azureml.sku_match import VCSelection
from azure_jobs.shared.job.spec import JobSpec
from azure_jobs.shared.opts import AmlOpts
from azure_jobs.shared.sku import MatchedInstances
from azure_jobs.shared.types.azure import VCInfo
from azure_jobs.shared.types.instance import InstanceTypeInfo


def _arm_client():
    return SimpleNamespace(
        quota=SimpleNamespace(get_by_name=MagicMock()),
        sku=SimpleNamespace(list=MagicMock()),
        ws=SimpleNamespace(get=MagicMock()),
        compute=SimpleNamespace(get_workspace=MagicMock()),
    )


class TestResolveTarget:
    def test_returns_backend_spec_unchanged_when_service_or_compute_do_not_apply(self):
        arm = _arm_client()
        aml = AmlOpts(compute="")

        assert resolve_target(
            JobSpec(name="job", service="aml", backend_spec=aml),
            arm_client=arm,
        ).aml is aml
        assert resolve_target(
            JobSpec(name="job", service="volcano", backend_spec=AmlOpts(compute="c")),
            arm_client=arm,
        ).aml.compute == "c"

        arm.quota.get_by_name.assert_not_called()
        arm.ws.get.assert_not_called()
        arm.compute.get_workspace.assert_not_called()
        with pytest.raises(AttributeError):
            resolve_target(
                JobSpec(
                    name="job",
                    service="volcano",
                    backend_spec=AmlOpts(compute="c"),
                ),
                arm_client=arm,
            ).compute

    def test_singularity_resolves_vc_and_falls_back_to_workspace_lookup(self):
        arm = _arm_client()
        vc = VCInfo(
            name="vc-name",
            subscription_id="vc-sub",
            resource_group="vc-rg",
            locations=["westus2"],
        )
        arm.quota.get_by_name.return_value = vc
        matched = MatchedInstances(
            instances=[
                InstanceTypeInfo(
                    name="Singularity.E16ads_v5",
                    series_id="Eadsv5",
                    num_cores=16,
                    accelerator="CPU",
                )
            ],
            effective_tier="Premium",
        )
        selection = VCSelection(
            vc=vc,
            matched_instances=matched,
            user_available=16,
            tier_available=16,
            required_capacity=16,
        )
        arm.ws.get.return_value = SimpleNamespace(
            subscription_id="ws-sub",
            resource_group="ws-rg",
        )
        aml = AmlOpts(compute="vc-name", workspace_name="ws-name")

        with patch(
            "azure_jobs.server.submit.azureml.sku_match.match_vc",
            return_value=selection,
        ):
            result = resolve_target(
                JobSpec(
                    name="job",
                    service="sing",
                    sku="1xC1",
                    backend_spec=aml,
                ),
                arm_client=arm,
            )

        assert result.aml.vc_subscription_id == "vc-sub"
        assert result.aml.vc_resource_group == "vc-rg"
        assert result.aml.subscription_id == "ws-sub"
        assert result.aml.resource_group == "ws-rg"
        arm.quota.get_by_name.assert_called_once_with("vc-name", strict=True)
        arm.ws.get.assert_called_once_with("ws-name")
        arm.compute.get_workspace.assert_not_called()

    def test_singularity_skips_workspace_lookup_when_workspace_coords_exist(self):
        arm = _arm_client()
        vc = VCInfo(
            name="vc-name",
            subscription_id="vc-sub",
            resource_group="vc-rg",
            locations=["westus2"],
        )
        arm.quota.get_by_name.return_value = vc
        aml = AmlOpts(
            compute="vc-name",
            workspace_name="ws-name",
            subscription_id="ws-sub",
            resource_group="ws-rg",
        )

        matched = MatchedInstances([], effective_tier="Premium")
        selection = VCSelection(vc, matched, 16, 16, 1)
        with patch(
            "azure_jobs.server.submit.azureml.sku_match.match_vc",
            return_value=selection,
        ):
            result = resolve_target(
                JobSpec(
                    name="job",
                    service="sing",
                    sku="1xC1",
                    backend_spec=aml,
                ),
                arm_client=arm,
            )

        assert result.aml.subscription_id == "ws-sub"
        assert result.aml.resource_group == "ws-rg"
        arm.ws.get.assert_not_called()

    def test_aml_resolves_workspace_from_compute_target(self):
        arm = _arm_client()
        arm.compute.get_workspace.return_value = SimpleNamespace(
            subscription_id="sub-1",
            resource_group="rg-1",
            name="ws-1",
        )
        aml = AmlOpts(compute="gpu-cluster")

        result = resolve_target(
            JobSpec(name="job", service="aml", backend_spec=aml),
            arm_client=arm,
        )

        assert result.aml.subscription_id == "sub-1"
        assert result.aml.resource_group == "rg-1"
        assert result.aml.workspace_name == "ws-1"
        arm.compute.get_workspace.assert_called_once_with("gpu-cluster")


class TestSubscriptionsApi:
    def test_list_returns_only_enabled_non_empty_subscription_ids(self):
        client = SimpleNamespace(
            get=MagicMock(
                return_value={
                    "value": [
                        {"subscriptionId": "sub-1", "state": "Enabled"},
                        {"subscriptionId": "sub-2", "state": "Disabled"},
                        {"subscriptionId": "", "state": "Enabled"},
                        {"state": "Enabled"},
                    ]
                }
            )
        )

        assert SubscriptionsAPI(client).list() == ["sub-1"]

    def test_list_tolerates_missing_value_array(self):
        client = SimpleNamespace(get=MagicMock(return_value={"unexpected": []}))

        assert SubscriptionsAPI(client).list() == []
