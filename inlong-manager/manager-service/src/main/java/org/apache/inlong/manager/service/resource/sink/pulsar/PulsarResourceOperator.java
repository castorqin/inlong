package org.apache.inlong.manager.service.resource.sink.pulsar;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.mapper.StreamSinkFieldEntityMapper;
import org.apache.inlong.manager.pojo.node.pulsar.PulsarDataNodeDTO;
import org.apache.inlong.manager.pojo.node.pulsar.PulsarDataNodeInfo;
import org.apache.inlong.manager.pojo.node.starrocks.StarRocksDataNodeInfo;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.sink.pulsar.PulsarSinkDTO;
import org.apache.inlong.manager.pojo.sink.starrocks.StarRocksSinkDTO;
import org.apache.inlong.manager.service.node.DataNodeOperateHelper;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarUtils;
import org.apache.inlong.manager.service.resource.sink.SinkResourceOperator;
import org.apache.inlong.manager.service.resource.sink.sqlserver.SQLServerResourceOperator;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * pulsar resource operate for creating pulsar resource
 */
@Service
public class PulsarResourceOperator implements SinkResourceOperator {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarResourceOperator.class);
    @Autowired
    private StreamSinkService sinkService;

    @Autowired
    private DataNodeOperateHelper dataNodeHelper;


    @Override
    public Boolean accept(String sinkType) {
        return null;
    }

    @Override
    public void createSinkResource(SinkInfo sinkInfo) {
        LOG.info("begin to create sink resources sinkId={}", sinkInfo.getId());
        if (SinkStatus.CONFIG_SUCCESSFUL.getCode().equals(sinkInfo.getStatus())) {
            LOG.warn("sink resource [" + sinkInfo.getId() + "] already success, skip to create");
            return;
        } else if (InlongConstants.DISABLE_CREATE_RESOURCE.equals(sinkInfo.getEnableCreateResource())) {
            LOG.warn("create resource was disabled, skip to create for [" + sinkInfo.getId() + "]");
            return;
        }
        this.createTopic(sinkInfo);
    }

    private void createTopic(SinkInfo sinkInfo) {
        PulsarSinkDTO pulsarSinkDTO = getPulsarDataNodeInfo(sinkInfo);
        PulsarAdmin pulsarAdmin;
        try {
            pulsarAdmin = PulsarUtils.getPulsarAdmin(pulsarSinkDTO.getAdminUrl(),
                    pulsarSinkDTO.getToken());
            Tenants tenants = pulsarAdmin.tenants();
            Clusters clusters = pulsarAdmin.clusters();
            Set<String> allowClusters = new HashSet<>(clusters.getClusters());
            if (!tenants.getTenants().contains(pulsarSinkDTO.getTenant())) {
                TenantInfo tenantInfo = TenantInfo.builder().allowedClusters(allowClusters)
                        .adminRoles(Collections.emptySet()).build();
                tenants.createTenant(pulsarSinkDTO.getTenant(), tenantInfo);
            }
            List<String> namespaces = pulsarAdmin.namespaces().getNamespaces(pulsarSinkDTO.getTenant());
            if (namespaces == null || namespaces.isEmpty() || !namespaces.contains(pulsarSinkDTO.getNamespace())) {
                pulsarAdmin.namespaces().createNamespace(pulsarSinkDTO.getNamespace());
            }
            pulsarAdmin.topics().createPartitionedTopic(pulsarSinkDTO.getTopic(), pulsarSinkDTO.getPartitionNum());
        } catch (PulsarClientException | PulsarAdminException e) {
            LOG.error("init pulsar admin error", e);
            throw new BusinessException();
        }

    }

    private PulsarSinkDTO getPulsarDataNodeInfo(SinkInfo sinkInfo) {
        PulsarSinkDTO pulsarSinkDTO = PulsarSinkDTO.getFromJson(sinkInfo.getExtParams());
        // read from data node if not supplied by user
        if (StringUtils.isBlank(pulsarSinkDTO.getAdminUrl())) {
            String dataNodeName = sinkInfo.getDataNodeName();
            Preconditions.expectNotBlank(dataNodeName, ErrorCodeEnum.INVALID_PARAMETER,
                    "pulsar admin url not specified and data node is empty");
            PulsarDataNodeInfo dataNodeInfo = (PulsarDataNodeInfo) dataNodeHelper.getDataNodeInfo(
                    dataNodeName, sinkInfo.getSinkType());
            CommonBeanUtils.copyProperties(dataNodeInfo, pulsarSinkDTO);
        }
        return pulsarSinkDTO;
    }
}
