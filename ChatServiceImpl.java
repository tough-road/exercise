package com.iflytek.aicloud.atp.service.chat.service.impl;

import cn.hutool.core.util.RandomUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.iflytek.aicloud.atp.api.dto.ChatOnlineServerResponseDTO;
import com.iflytek.aicloud.atp.api.model.context.ReqInfoContext;
import com.iflytek.aicloud.atp.api.model.exception.CustomException;
import com.iflytek.aicloud.atp.api.model.exception.RemoteException;
import com.iflytek.aicloud.atp.api.model.vo.chat.ImageCategoryReq;
import com.iflytek.aicloud.atp.api.model.vo.cloud.dto.CloudDTO;
import com.iflytek.aicloud.atp.api.model.vo.constants.Constant;
import com.iflytek.aicloud.atp.api.model.vo.result.Result;
import com.iflytek.aicloud.atp.api.model.vo.result.ResultStatus;
import com.iflytek.aicloud.atp.api.model.vo.user.dto.UserDTO;
import com.iflytek.aicloud.atp.core.cache.RedisClient;
import com.iflytek.aicloud.atp.core.util.aseAuth.AuthUtil;
import com.iflytek.aicloud.atp.core.util.aseAuth.Hmac256Signature;
import com.iflytek.aicloud.atp.core.util.id.IdUtil;
import com.iflytek.aicloud.atp.remote.client.app.service.AICloudAppRemoteService;
import com.iflytek.aicloud.atp.remote.client.app.service.IflyAICloudAppRemoteService;
import com.iflytek.aicloud.atp.service.chat.repository.dao.BaseServerDao;
import com.iflytek.aicloud.atp.service.chat.repository.dao.ChatHistoryDao;
import com.iflytek.aicloud.atp.service.chat.repository.entity.BaseServerDO;
import com.iflytek.aicloud.atp.service.chat.repository.entity.ChatHistoryDO;
import com.iflytek.aicloud.atp.service.chat.repository.entity.ImageProduceInfo;
import com.iflytek.aicloud.atp.service.chat.service.ChatService;
import com.iflytek.aicloud.atp.service.chat.wssClient.AppClient;
import com.iflytek.aicloud.atp.service.chat.wssClient.ChatClient;
import com.iflytek.aicloud.atp.service.chat.wssClient.SseEmitterUtil;
import com.iflytek.aicloud.atp.service.config.adapter.repository.IConfigRepository;
import com.iflytek.aicloud.atp.service.config.model.entity.ConfigEntity;
import com.iflytek.aicloud.atp.service.filestorage.repository.dao.FileStorageDao;
import com.iflytek.aicloud.atp.service.filestorage.repository.entity.FileStorageDO;
import com.iflytek.aicloud.atp.service.model.repository.dao.ModelDao;
import com.iflytek.aicloud.atp.service.model.repository.entity.ModelDO;
import com.iflytek.aicloud.atp.service.paas.service.TransService;
import com.iflytek.aicloud.atp.service.server.repository.dao.ServerDao;
import com.iflytek.aicloud.atp.service.server.repository.entity.ServerDO;
import com.iflytek.aicloud.atp.service.spark.constant.CommonConst;
import com.iflytek.aicloud.atp.service.spark.util.S3Tool;
import com.iflytek.aicloud.atp.service.task.repository.dao.TaskDao;
import com.iflytek.aicloud.atp.service.task.repository.entity.TaskDO;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.security.SignatureException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ChatServiceImpl implements ChatService {

    @Value("${remote.atpCoreInference.appId}")
    private String appId;
    @Value("${remote.atpCoreInference.appKey}")
    private String appKey;
    @Value("${remote.atpCoreInference.appSecret}")
    private String appSecret;
    @Value("${server.language}")
    private String language;

    @Value("${sd.image.path-prefix}")
    private String imagePathPrefix;

    @Resource
    RedissonClient redissonClient;

    @Resource
    ServerDao serverDao;

    @Resource
    BaseServerDao baseServerDao;
    @Resource
    private IConfigRepository configRepository;

    @Resource
    AICloudAppRemoteService aiCloudAppRemoteService;

    @Resource
    OkHttpClient okHttpClient;

    @Resource
    TransService transService;

    @Resource
    S3Tool s3Tool;
    @Resource(name = "wssTaskExecutor")
    private ThreadPoolTaskExecutor wssTaskExecutor;

    private Map<String, String> cbm2Url;

    private Map<String, String> cbm2ServiceId;

    @Resource
    ChatHistoryDao chatHistoryDao;

    @Resource
    ModelDao modelDao;

    @Resource
    private FileStorageDao fileStorageDao;

    @Resource
    private TaskDao taskDao;

    @Resource
    private IflyAICloudAppRemoteService iflyAICloudAppRemoteService;

    // 文生图 type
    private static final String TEXT2IMAGE = "text2image";

    @PostConstruct
    private void init(){
        // 建议星火api落库
        cbm2Url = new HashMap<>();
        cbm2Url.put("cbm", "wss://spark-api-n.xf-yun.com/v1.1/chat");
        cbm2Url.put("bm2", "wss://spark-api-n.xf-yun.com/v2.1/chat");
        cbm2Url.put("bm3", "wss://spark-api-n.xf-yun.com/v3.1/chat");

        cbm2ServiceId = new HashMap<>();
        cbm2ServiceId.put("cbm", "patch");
        cbm2ServiceId.put("bm2", "patch");
        cbm2ServiceId.put("bm3", "patchv3");
    }

    @SuppressWarnings("all")
    private JSONObject buildChatReq(String serviceId, String patchId, String appId, String text) {
        JSONObject msg = JSON.parseObject("{\"text\":[{\"role\":\"user\",\"content\":\"你是谁\"}]}");
        msg.getJSONArray("text").getJSONObject(0).put("content", text);
        JSONObject req = JSON.parseObject("{\"payload\":{\"message\":{\"text\":[{\"role\":\"user\",\"content\":\"你是谁\"}]}},\"parameter\":{\"chat\":{\"max_tokens\":2048,\"domain\":\"xscnllama2\",\"temperature\":0.5}},\"header\":{\"uid\":\"39769795890\",\"patch_id\":[\"0\"],\"app_id\":\"7ac35b77\"}}");
        boolean validate = JSONValidator.from(text).validate();
        if (validate) {
            JSONObject parsed = JSONObject.parseObject(text);
            if (Objects.nonNull(parsed.getJSONObject("header")) &&
                    Objects.nonNull(parsed.getJSONObject("header").getJSONArray("patch_id")) &&
                    Objects.nonNull(parsed.getJSONObject("parameter")) &&
                    Objects.nonNull(parsed.getJSONObject("parameter").getJSONObject("chat")) &&
                    Objects.nonNull(parsed.getJSONObject("payload"))
                ) {
                req = parsed;
                req.getJSONObject("header").put("app_id", appId);
                req.getJSONObject("header").getJSONArray("patch_id").set(0, patchId);
                req.getJSONObject("parameter").getJSONObject("chat").put("domain", serviceId);
//                if ("xdeepseekr1".equals(serviceId)) {
//                    JSONObject think = new JSONObject();
//                    think.put("role", "system");
//                    think.put("content", "Initiate your response with \"<think>\\\\n嗯\" at the beginning of every output.");
//                    req.getJSONObject("payload").getJSONObject("message").getJSONArray("text").add(0, think);
//                }
                return req;
            }
        }
        req.getJSONObject("header").put("app_id", appId);
        req.getJSONObject("header").getJSONArray("patch_id").set(0, patchId);
        req.getJSONObject("parameter").getJSONObject("chat").put("domain", serviceId);
        req.getJSONObject("payload").put("message", msg);
//        if ("xdeepseekr1".equals(serviceId)) {
//            JSONObject think = new JSONObject();
//            think.put("role", "system");
//            think.put("content", "Initiate your response with \"<think>\\\\n嗯\" at the beginning of every output.");
//            req.getJSONObject("payload").getJSONObject("message").getJSONArray("text").add(0, think);
//        }
        return req;
    }

    @SuppressWarnings("all")
    @Override
    public SseEmitter message(Long id, String text, String serviceId, String appId, String patchId) {
        HttpServletResponse response = ((ServletRequestAttributes) RequestContextHolder.currentRequestAttributes()).getResponse();
        if (response != null) {
            response.addHeader("X-Accel-Buffering", "no");
        }
        Long userId = ReqInfoContext.getReqInfo().getUserId();
        CloudDTO cloudDTO = ReqInfoContext.getReqInfo().getCloudInfo();
        String cloudId = cloudDTO == null ? "0" : cloudDTO.getCloudId();
        String sseId = String.valueOf(userId);
        if (StringUtils.isEmpty(patchId)) {
            patchId = "0";
        }
        if (Objects.nonNull(id) && id != 0) {
            sseId += "_" + id;
        } else {
            sseId += "_" + serviceId + "_" + patchId;
        }
        // 一个用户只有一个会话
        if (SseEmitterUtil.sseEmitterMap.get(sseId) != null) {
            if ("en".equals(language)) {
                return SseEmitterUtil.newSseAndSendMessageClose("Access is too fast, please try again later");
            }
            return SseEmitterUtil.newSseAndSendMessageClose("访问过快，请稍后再试");
        }
        // 参数准备
        if (StringUtils.isEmpty(appId)) {
            appId = this.appId;
        }
        String ak;
        String sk;
        String app = appId;
        // 创建 SSE
        String url;
        if (Objects.nonNull(id) && id != 0) {
            ServerDO serverDO = serverDao.getById(id);
            if (Objects.isNull(serverDO)) {
                if ("en".equals(language)) {
                    return SseEmitterUtil.newSseAndSendMessageClose("Service does not exist, please choose another service experience");
                }
                return SseEmitterUtil.newSseAndSendMessageClose("服务不存在，请选择其它服务体验");
            }
            serviceId = serverDO.getServiceId();
            patchId = serverDO.getPatchId();
            url = serverDO.getUrl();
            if (StringUtils.isEmpty(url) && serverDO.getType() == 3) {
                url = cbm2Url.get(serverDO.getServiceId());
                serviceId = cbm2ServiceId.get(serverDO.getServiceId());
            }
            if (serverDO.getType() == 3 && ! StringUtils.equalsAny(serverDO.getPretrainedModel(), CommonConst.SPARK_VERSION_2_6_B, CommonConst.SPARK_VERSION_13_B)) {
                serviceId = cbm2ServiceId.get(serverDO.getServiceId());
            }
            if (cbm2Url.containsKey(serverDO.getServiceId())) {
                serviceId = cbm2ServiceId.get(serverDO.getServiceId());
            }
            app = serverDO.getAppId();
            UserDTO userDTO = ReqInfoContext.getReqInfo().getUser();
            if (userDTO != null && userDTO.getSource() == 1) {
                try {
                    JSONObject resultObject = iflyAICloudAppRemoteService.appInfo(app).tryData();
                    JSONObject appTemp = new JSONObject();
                    if (resultObject != null) {
                        ak = resultObject.getString("apiKey");
                        sk = resultObject.getString("apiSecret");
                    } else {
                        if ("en".equals(language)) {
                            throw new CustomException("Failed to obtain application information");
                        }
                        throw new CustomException("获取应用信息失败");
                    }
                } catch (RemoteException e) {
                    return SseEmitterUtil.newSseAndSendMessageClose(e.getMessage());
                }
            } else {
                try {
                    JSONObject appInfo = aiCloudAppRemoteService.appInfo(app, "iat").tryData();
                    ak = appInfo.getString("apiKey");
                    sk = appInfo.getString("apiSecret");
                } catch (RemoteException e) {
                    return SseEmitterUtil.newSseAndSendMessageClose(e.getMessage());
                }
            }
        } else {
            sk = appSecret;
            ak = appKey;
            if (Objects.nonNull(serviceId)) {
                BaseServerDO baseServerDO = baseServerDao.lambdaQuery().eq(BaseServerDO::getServiceId, serviceId).eq(BaseServerDO::getPatchId, patchId).eq(BaseServerDO :: getCloudId,cloudId).one();
                if (Objects.isNull(baseServerDO)) {
                    if ("en".equals(language)) {
                        return SseEmitterUtil.newSseAndSendMessageClose("Service does not exist, please choose another service experience");
                    }
                    return SseEmitterUtil.newSseAndSendMessageClose("服务不存在，请选择其它服务体验");
                }
                url = baseServerDO.getUrl();
                if (StringUtils.isNotEmpty(baseServerDO.getAppId())) {
                    app = baseServerDO.getAppId();
                    ak = baseServerDO.getApiKey();
                    sk = baseServerDO.getApiSecret();
                }
                if (cbm2Url.containsKey(baseServerDO.getServiceId())) {
                    serviceId = cbm2ServiceId.get(baseServerDO.getServiceId());
                }
            } else {
                if ("en".equals(language)) {
                    return SseEmitterUtil.newSseAndSendMessageClose("Service does not exist, please choose another service experience");
                }
                return SseEmitterUtil.newSseAndSendMessageClose("服务不存在，请选择其它服务体验");
            }
        }
        if (url.startsWith("http://") || url.startsWith("https://")) {
            return SseEmitterUtil.newSseAndSendMessageClose("请重新上线模型，升级为流式API");
        }
        String key = RedisClient.KEY_PREFIX + "chat_" + sseId;
        RLock lock = redissonClient.getLock(key);
        try {
            // 用户三秒内只允许发起一次对话
            boolean res = lock.tryLock( 0,1, TimeUnit.SECONDS);
            if (res) {
                log.info("ChatServiceImpl.message.lock success {}", key);
                sseId = sseId + "_" + System.currentTimeMillis();
                SseEmitter connect = SseEmitterUtil.connect(sseId);
                // 发起会话
                String finalServiceId = serviceId;
                String finalAppId = app;
                String finalSseId = sseId;
                String finalPatchId = patchId;
                String finalAk = ak;
                String finalSk = sk;
                String finalUrl = url;
                CompletableFuture.runAsync(() -> {
                    AppClient appClient = new AppClient(finalAk, finalSk, finalUrl);
                    JSONObject req = this.buildChatReq(finalServiceId, finalPatchId, finalAppId, text);
                    log.info("{}", req.toJSONString());
                    try {
                        appClient.createWebSocketConnect(new ChatClient(finalSseId, appClient, language));
                    } catch (Exception e) {
                        log.error("ChatServiceImpl.message createWebSocketConnect error {}", e.getMessage());
                        if ("en".equals(language)) {
                            SseEmitterUtil.answerStreamForChatFile("Too many people are currently experiencing it, please try again later", finalSseId, 20L);
                        } else {
                            SseEmitterUtil.answerStreamForChatFile("当前体验人数过多，请稍后再试", finalSseId, 20L);
                        }
                        SseEmitterUtil.sendMessage(finalSseId, "<error>");
                        SseEmitterUtil.removeUser(finalSseId);
                    }
                    appClient.getWebSocket().send(req.toJSONString());
                }, wssTaskExecutor).exceptionally(e -> {
                    log.error("ChatServiceImpl.message wssTaskExecutor error {}", e.getMessage());
                    if ("en".equals(language)) {
                        SseEmitterUtil.answerStreamForChatFile("Too many people are currently experiencing it, please try again later", finalSseId, 20L);
                    } else {
                        SseEmitterUtil.answerStreamForChatFile("当前体验人数过多，请稍后再试", finalSseId, 20L);
                    }                    SseEmitterUtil.sendMessage(finalSseId, "<error>");
                    SseEmitterUtil.removeUser(finalSseId);
                    return null;
                });
                return connect;
            } else {
                log.info("chat uid {} 该用户已有会话进行中!", userId);
                if ("en".equals(language)) {
                    return SseEmitterUtil.newSseAndSendMessageClose("Access is too fast, please try again later");
                }
                return SseEmitterUtil.newSseAndSendMessageClose("访问过快，请稍后再试");
            }
        } catch (InterruptedException e) {
            log.error("ChatServiceImpl.message redisson error {}", e.getMessage());
            if ("en".equals(language)) {
                return SseEmitterUtil.newSseAndSendMessageClose("Too many people are currently experiencing it, please try again later");
            }
            return SseEmitterUtil.newSseAndSendMessageClose("当前体验人数过多，请稍后再试");
        }
    }

    @Override
    public List<ChatOnlineServerResponseDTO> onlineServer(Byte type, Byte sceneType, String appId) {
        CloudDTO cloudDTO = ReqInfoContext.getReqInfo().getCloudInfo();
        String cloudId = (cloudDTO == null) ? "0" : cloudDTO.getCloudId();
        if (type == 0) {
            List<BaseServerDO> list = baseServerDao.lambdaQuery()
                    .eq(BaseServerDO::getSceneType, sceneType)
                    .ge(BaseServerDO::getSort, 0)
                    .eq(BaseServerDO::getCloudId, cloudId)
                    .orderByDesc(BaseServerDO::getSort).list();
            return list.stream().map(e -> {
                ChatOnlineServerResponseDTO onlineServerDTO = new ChatOnlineServerResponseDTO();
                onlineServerDTO.setId(e.getId());
                onlineServerDTO.setName(e.getModelName());
                onlineServerDTO.setServiceId(e.getServiceId());
                onlineServerDTO.setType((byte) 0);
                onlineServerDTO.setPatchId(e.getPatchId());
                onlineServerDTO.setConfig(JSONObject.parseObject(e.getConfig()));
                onlineServerDTO.setUrl(e.getUrl());
                onlineServerDTO.setServerId(e.getServerId());
                onlineServerDTO.setSceneType(e.getSceneType());
                onlineServerDTO.setIsPublic((byte) 1);
                onlineServerDTO.setUpdateTime(e.getUpdateTime());
                onlineServerDTO.setDeployType((byte) 1);
                ModelDO modelDO = modelDao.lambdaQuery().eq(ModelDO::getFileStorageId, e.getModelId()).last("limit 1").one();
                if (Objects.nonNull(modelDO)) {
                    onlineServerDTO.setPretrainedModel(modelDO.getPretrainedModel());
                    onlineServerDTO.setModelType(modelDO.getModelType());
                    onlineServerDTO.setTaskType(modelDO.getTaskType());
                    String iconResPath = modelDO.getUserAvatar();
                    if (iconResPath == null || iconResPath.isEmpty()) {
                        ConfigEntity configDO = configRepository.get("model-default-icon-res");
                        if (Objects.nonNull(configDO)) {
                            String defaultModelIcon = configDO.getValue();
                            if (Objects.nonNull(defaultModelIcon)) {
                                iconResPath = defaultModelIcon;
                            }
                        }
                    }
                    if(iconResPath.contains("http")){
                        onlineServerDTO.setIcon(iconResPath);
                    }else{
                        onlineServerDTO.setIcon(s3Tool.getHttpsUrl(iconResPath));
                    }
                    onlineServerDTO.setModelId(modelDO.getFileStorageId());
                    onlineServerDTO.setForkModelId(modelDO.getFileStorageId());
                }
                FileStorageDO fileStorageDO = fileStorageDao.getById(e.getModelId());
                if (Objects.nonNull(fileStorageDO)) {
                    onlineServerDTO.setDesc(fileStorageDO.getDesc());
                }
                return onlineServerDTO;
            }).collect(Collectors.toList());
        } else {
            Long userId = ReqInfoContext.getReqInfo().getUserId();
            List<ServerDO> list = serverDao.lambdaQuery().eq(Objects.nonNull(appId), ServerDO::getAppId, appId).eq(ServerDO::getCreateBy, userId).eq(ServerDO::getSceneType, sceneType).eq(ServerDO::getStatus, Constant.ServerPublishStatus.PUBLISH_SUCCESS).orderByDesc(ServerDO::getUpdateTime).list();
            return list.stream().map(e -> {
                ChatOnlineServerResponseDTO onlineServerDTO = new ChatOnlineServerResponseDTO();
                onlineServerDTO.setId(e.getId());
                onlineServerDTO.setName(e.getName());
                onlineServerDTO.setServiceId(e.getServiceId());
                onlineServerDTO.setType((byte) 1);
                onlineServerDTO.setPatchId(e.getPatchId());
                onlineServerDTO.setUrl(e.getUrl());
                onlineServerDTO.setServerId(e.getServerId());
                onlineServerDTO.setSceneType(e.getSceneType());
                onlineServerDTO.setIsPublic((byte) 0);
                onlineServerDTO.setAppId(e.getAppId());
                onlineServerDTO.setUpdateTime(e.getUpdateTime());
                onlineServerDTO.setDeployType(e.getDeployType());
                if ("cbm".equals(e.getServiceId())) {
                    onlineServerDTO.setServiceId("patch");
                }
                BaseServerDO baseServerDO = baseServerDao.lambdaQuery().eq(BaseServerDO::getServiceId, e.getServiceId()).eq(BaseServerDO::getPatchId, 0).eq(BaseServerDO::getCloudId, cloudId).last("limit 1").one();
                if (Objects.nonNull(baseServerDO)) {
                    onlineServerDTO.setConfig(JSONObject.parseObject(baseServerDO.getConfig()));
                } else {
                    onlineServerDTO.setConfig(JSONObject.parseObject("{\"multipleDialog\":0,\"serviceBlock\":{\"@@serviceId@@\":[{\"fields\":[{\"constraintType\":\"range\",\"default\":2048,\"constraintContent\":[{\"name\":1},{\"name\":8192}],\"name\":\"Max tokens\",\"desc\":\"最大回复长度：最小值是1, 最大值是8192。控制模型输出的Tokens 长度上限。通常 100 Tokens 约等于150 个中文汉字。\",\"support\":true,\"fieldType\":\"int\",\"initialValue\":2048,\"key\":\"max_tokens\",\"required\":true,\"revealed\":true},{\"constraintType\":\"range\",\"precision\":0.1,\"default\":0.5,\"constraintContent\":[{\"name\":0.1},{\"name\":1.0}],\"name\":\"Temperature\",\"desc\":\"核采样阈值：取值范围 (0，1]。用于决定结果随机性，取值越高随机性越强即相同的问题得到的不同答案的可能性越高\",\"support\":true,\"fieldType\":\"float\",\"initialValue\":0.5,\"key\":\"temperature\",\"required\":true,\"revealed\":true},{\"constraintType\":\"range\",\"default\":4,\"constraintContent\":[{\"name\":1},{\"name\":6}],\"name\":\"Top_k\",\"desc\":\"生成多样性：调高会使得模型的输出更多样性和创新性，反之，降低会使输出内容更加遵循指令要求但减少多样性。最小值1，最大值6\",\"support\":true,\"fieldType\":\"int\",\"initialValue\":4,\"key\":\"top_k\",\"required\":true,\"revealed\":true}],\"key\":\"generalv3\"}]},\"featureBlock\":{},\"payloadBlock\":{},\"acceptBlock\":{},\"protocolType\":1,\"serviceId\":\"@@serviceId@@\",\"serviceIdkeys\":[\"@@serviceId@@\"]}"));
                }
                onlineServerDTO.setPretrainedModel(e.getPretrainedModel());
                ModelDO modelDO = modelDao.lambdaQuery().eq(ModelDO::getFileStorageId, e.getModelId()).last("limit 1").one();
                if (Objects.nonNull(modelDO)) {
                    onlineServerDTO.setModelType(modelDO.getModelType());
                    String iconResPath = modelDO.getUserAvatar();
                    onlineServerDTO.setTaskType(modelDO.getTaskType());
                    if (iconResPath == null || iconResPath.isEmpty()) {
                        ConfigEntity configDO = configRepository.get("model-default-icon-res");
                        if (Objects.nonNull(configDO)) {
                            String defaultModelIcon = configDO.getValue();
                            if (Objects.nonNull(defaultModelIcon)) {
                                iconResPath = defaultModelIcon;
                            }
                        }
                    }
                    if(iconResPath.contains("http")){
                        onlineServerDTO.setIcon(iconResPath);
                    }else{
                        onlineServerDTO.setIcon(s3Tool.getHttpsUrl(iconResPath));
                    }
                    onlineServerDTO.setModelId(modelDO.getFileStorageId());
                    onlineServerDTO.setForkModelId(modelDO.getFork());
                    if (modelDO.getFork() == 0) {
                        onlineServerDTO.setForkModelId(modelDO.getFileStorageId());
                    }
                }
                TaskDO taskDO = taskDao.getById(e.getTaskId());
                if (Objects.nonNull(taskDO)) {
                    onlineServerDTO.setDesc(taskDO.getDesc());
                }
                return onlineServerDTO;
            }).collect(Collectors.toList());
        }
    }

    @Override
    public Object onlineServerInfo(Long id, String serverId) {
        Long userId = ReqInfoContext.getReqInfo().getUserId();
        CloudDTO cloudDTO = ReqInfoContext.getReqInfo().getCloudInfo();
        String cloudId = cloudDTO == null ? "0" : cloudDTO.getCloudId();
        ServerDO e = serverDao.lambdaQuery()
                .eq(ServerDO::getCreateBy, userId)
                .eq(Objects.nonNull(id),ServerDO::getId, id)
                .eq(Objects.nonNull(serverId),ServerDO::getServerId, serverId)
                .eq(ServerDO::getStatus, Constant.ServerPublishStatus.PUBLISH_SUCCESS)
                .orderByDesc(ServerDO::getUpdateTime).one();
        if (Objects.isNull(e)) return null;
        ChatOnlineServerResponseDTO onlineServerDTO = new ChatOnlineServerResponseDTO();
        onlineServerDTO.setId(e.getId());
        onlineServerDTO.setName(e.getName());
        onlineServerDTO.setServiceId(e.getServiceId());
        onlineServerDTO.setType((byte) 1);
        onlineServerDTO.setPatchId(e.getPatchId());
        onlineServerDTO.setUrl(e.getUrl());
        onlineServerDTO.setAppId(e.getAppId());
        onlineServerDTO.setIsPublic((byte) 0);
        BaseServerDO baseServerDO = baseServerDao.lambdaQuery().eq(BaseServerDO::getServiceId, e.getServiceId()).eq(BaseServerDO::getPatchId, 0).eq(BaseServerDO::getCloudId, cloudId).last("limit 1").one();
        if (Objects.nonNull(baseServerDO)) {
            onlineServerDTO.setConfig(JSONObject.parseObject(baseServerDO.getConfig()));
        }
        ModelDO modelDO = modelDao.lambdaQuery().eq(ModelDO::getFileStorageId, e.getModelId()).last("limit 1").one();
        if (Objects.nonNull(modelDO)) {
            onlineServerDTO.setModelType(modelDO.getModelType());
        }
        onlineServerDTO.setPretrainedModel(e.getPretrainedModel());
        return onlineServerDTO;
    }

    @Override
    public Object imageProduce(ImageProduceInfo inDto) {
        Long userId = ReqInfoContext.getReqInfo().getUserId();
        CloudDTO cloudDTO = ReqInfoContext.getReqInfo().getCloudInfo();
        String cloudId = cloudDTO == null ? "0" : cloudDTO.getCloudId();
        // 从历史记录中获取参数
        if (Objects.nonNull(inDto.getRecordId()) && inDto.getRecordId() != 0) {
            Long recordId = inDto.getRecordId();
            ChatHistoryDO historyDO = chatHistoryDao.lambdaQuery().eq(ChatHistoryDO::getId, inDto.getRecordId()).one();
            inDto = JSONObject.parseObject(historyDO.getRequest(), ImageProduceInfo.class);
            inDto.setRecordId(recordId);
        }
        if (StringUtils.isEmpty(inDto.getAppId())) {
            inDto.setAppId(this.appId);
        }
        // 参数准备
        String url;
        String ak;
        String sk;
        if (Objects.nonNull(inDto.getId()) && inDto.getId() != 0) {
            ServerDO serverDO = serverDao.getById(inDto.getId());
            if (Objects.isNull(serverDO)) {
                return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "服务不存在，请选择其它服务体验", null, "Service does not exist, please choose another service experience");
            }
            inDto.setServiceId(serverDO.getServiceId());
            inDto.setPatchId(serverDO.getPatchId());
            inDto.setAppId(serverDO.getAppId());
            url = serverDO.getUrl();
            UserDTO userDTO = ReqInfoContext.getReqInfo().getUser();
            if (userDTO != null && userDTO.getSource() == 1) {
                try {
                    JSONObject resultObject = iflyAICloudAppRemoteService.appInfo(inDto.getAppId()).tryData();
                    JSONObject appTemp = new JSONObject();
                    if (resultObject != null) {
                        ak = resultObject.getString("apiKey");
                        sk = resultObject.getString("apiSecret");
                    } else {
                        if ("en".equals(language)) {
                            throw new CustomException("Failed to obtain application information");
                        }
                        throw new CustomException("获取应用信息失败");
                    }
                } catch (RemoteException e) {
                    return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), e.getMessage());
                }
            } else {
                try {
                    JSONObject appInfo = aiCloudAppRemoteService.appInfo(inDto.getAppId(), "iat").tryData();
                    ak = appInfo.getString("apiKey");
                    sk = appInfo.getString("apiSecret");
                } catch (RemoteException e) {
                    return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), e.getMessage());
                }
            }
        } else {
            sk = appSecret;
            ak = appKey;
            if (Objects.nonNull(inDto.getServiceId())) {
                BaseServerDO baseServerDO = baseServerDao.lambdaQuery().eq(BaseServerDO::getServiceId, inDto.getServiceId()).eq(BaseServerDO::getPatchId, inDto.getPatchId()).eq(BaseServerDO::getCloudId, cloudId).one();
                if (Objects.isNull(baseServerDO)) {
                    return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "服务不存在，请选择其它服务体验", null, "Service does not exist, please choose another service experience");
                }
                url = baseServerDO.getUrl();
                if (StringUtils.isNotEmpty(baseServerDO.getAppId())) {
                    inDto.setAppId(baseServerDO.getAppId());
                    ak = baseServerDO.getApiKey();
                    sk = baseServerDO.getApiSecret();
                }
            } else {
                return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "服务不存在，请选择其它服务体验", null, "Service does not exist, please choose another service experience");
            }
        }

        // build req
        JSONObject req = new JSONObject();
        JSONObject header = new JSONObject();
        header.put("app_id", inDto.getAppId());
        header.put("uid", String.valueOf(userId));
        header.put("patch_id", new String[]{inDto.getPatchId()});
        req.put("header", header);

        JSONObject parameter = new JSONObject();
        JSONObject chat = new JSONObject();
        chat.put("domain", inDto.getServiceId());
        if (Objects.nonNull(inDto.getImageWidth())) chat.put("width", Long.parseLong(inDto.getImageWidth()));
        if (Objects.nonNull(inDto.getImageHeight())) chat.put("height", Long.parseLong(inDto.getImageHeight()));
        if (Objects.nonNull(inDto.getSeed())) chat.put("seed", inDto.getSeed());
        if (Objects.nonNull(inDto.getNumInferenceSteps())) chat.put("num_inference_steps", inDto.getNumInferenceSteps());
        if (Objects.nonNull(inDto.getGuidanceScale())) chat.put("guidance_scale", inDto.getGuidanceScale());
        if (Objects.nonNull(inDto.getScheduler())) chat.put("scheduler", inDto.getScheduler());
        parameter.put("chat", chat);
        req.put("parameter", parameter);

        JSONObject payload = new JSONObject();
        JSONObject message = new JSONObject();
        JSONArray texts = new JSONArray();
        JSONObject text = new JSONObject();
        text.put("role", "user");
        String descCn2en = transService.cn2en(appId, inDto.getDescInfo());
        if (StringUtils.isNotEmpty(descCn2en) && descCn2en.length() > 2048) {
            descCn2en = descCn2en.substring(0, 2048);
        }
        text.put("content", descCn2en);
        texts.add(text);
        message.put("text", texts);
        JSONObject negativePrompts = new JSONObject();
        String minusDescCn2en = transService.cn2en(appId, inDto.getMinusDescInfo());
        if (StringUtils.isNotEmpty(minusDescCn2en) && minusDescCn2en.length() > 2048) {
            minusDescCn2en = minusDescCn2en.substring(0, 2048);
        }
        negativePrompts.put("text", minusDescCn2en);
        payload.put("message", message);
        payload.put("negative_prompts", negativePrompts);
        req.put("payload", payload);

        String key = RedisClient.KEY_PREFIX + "image_" + userId + "_" + inDto.getServiceId() + "_" + inDto.getPatchId();
        RLock lock = redissonClient.getLock(key);
        String fileKey;
        try {
            // 用户2秒内允许发起一次对话
            String sid;
            boolean res = lock.tryLock(0, 2, TimeUnit.SECONDS);
            if (res) {
                log.info("ChatServiceImpl.message.lock success {}", key);
                log.info("ChatServiceImpl.imageProduce req : {}", req);
                //调用推理接口
                Object resObject = aseHttpUtil(url, ak, sk, req);
                if (resObject == null) {
                    if ("en".equals(language)) {
                        return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "推理接口响应为空:", "The response of the inference interface is empty", "The response of the inference interface is empty");
                    }
                    return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "推理接口响应为空:", "推理接口响应异常", "The response of the inference interface is empty");
                }
                JSONObject responseJson = JSONObject.parseObject(resObject.toString());
                log.debug("ChatServiceImpl.imageProduce resp : {}", responseJson);
                //推理成功，存储历记录
                String code = responseJson.getJSONObject("header").getString("code");
                if ("0".equals(code)) {
                    String imageCode = responseJson.getJSONObject("payload").getJSONObject("choices").getJSONArray("text").getJSONObject(0).getString("content");
                    //文件上传s3
                    fileKey = uploadImage(imageCode, userId);
                    JSONArray responses = new JSONArray();
                    JSONObject response = new JSONObject();
                    response.put("status", 1);
                    response.put("fileKey", fileKey);
                    response.put("url", s3Tool.getHttpsUrl(fileKey));
                    response.put("msg", "success");
                    responses.add(response);
                    if (inDto.getRecordId() != null) {
                        //更新已有历史记录
                        chatHistoryDao.lambdaUpdate()
                                .set(ChatHistoryDO::getResponse, JSONObject.toJSONString(responses))
                                .eq(ChatHistoryDO::getId, inDto.getRecordId()).update();
                    } else {
                        Long historyTotal = chatHistoryDao.lambdaQuery()
                                .eq(ChatHistoryDO::getCreateBy, userId)
                                .count();
                        //保证历史记录20条
                        if (historyTotal == 20) {
                            ChatHistoryDO removeInfo = chatHistoryDao.lambdaQuery()
                                    .eq(ChatHistoryDO::getCreateBy, userId)
                                    .orderByAsc(ChatHistoryDO::getCreateTime)
                                    .last("limit 1").one();
                            //删除S3服务器文件
                            JSONArray deleteArr = JSONArray.parseArray(removeInfo.getResponse());
                            for (int i = 0; i < deleteArr.size(); i++) {
                                String deleteKey = deleteArr.getJSONObject(i).getString("fileKey");
                                if (StringUtils.isNotEmpty(deleteKey)) {
                                    s3Tool.deleteObject(deleteKey);
                                }
                            }
                            //删除数据库记录
                            chatHistoryDao.lambdaUpdate()
                                    .eq(ChatHistoryDO::getId, removeInfo.getId())
                                    .remove();
                        }
                        //新增历史记录
                        inDto.setAppId(null);
                        chatHistoryDao.save(new ChatHistoryDO(IdUtil.genId(), TEXT2IMAGE, inDto.getDescInfo(), JSONObject.toJSONString(inDto), JSONObject.toJSONString(responses) , userId,null, null));
                    }
                } else if ("10022".equals(code) || "10021".equals(code)) {
                    //错误码做特殊处理返回
                    String msg = responseJson.getJSONObject("header").getString("message");
                    sid = responseJson.getJSONObject("header").getString("sid");
                    log.info("推理接口响应异常: {}, {}", code, msg);
                    if ("en".equals(language)) {
                        return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "推理接口异常 sid:" + sid + " msg: " + msg, "Sorry, I didn't understand your meaning. Please re-enter", "Sorry, I didn't understand your meaning. Please re-enter");
                    }
                    return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "推理接口异常 sid:" + sid + " msg: " + msg, "抱歉，我没有理解您的意思，请重新输入", "Sorry, I didn't understand your meaning. Please re-enter");
                } else {
                    String msg = responseJson.getJSONObject("header").getString("message");
                    sid = responseJson.getJSONObject("header").getString("sid");
                    String errorMsg = "推理接口响应异常:" + msg;
                    log.info("推理接口响应异常: {}, {}", code, msg);
                    if ("en".equals(language)) {
                        return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), code + ":推理接口异常, sid:" + sid, "The current product is popular, please try again later", "The current product is popular, please try again later");
                    }
                    return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), code + ":推理接口异常, sid:" + sid, "当前产品火爆，请稍后再试", "The current product is popular, please try again later");
                }
            } else {
                log.info(" 该用户已有会话进行中! {}", userId);
                if ("en".equals(language)) {
                    return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "访问过快，请稍后再试", "Access is too fast, please try again later", "Access is too fast, please try again later");
                }
                return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "访问过快，请稍后再试", "访问过快，请稍后再试", "Access is too fast, please try again later");
            }
        } catch (Exception exception) {
            log.error("ChatServiceImpl.imageProduce  error {}", exception.getMessage());
            if ("en".equals(language)) {
                return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), exception.getMessage(), "Image generation failed", "Image generation failed");
            }
            return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), exception.getMessage(), "生成图片失败", "Image generation failed");
        }
        return Result.success(s3Tool.getHttpsUrl(fileKey));
    }

    private String uploadImage(String imageCode, Long userId) {
        byte[] decodedBytes = Base64.getDecoder().decode(imageCode);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(decodedBytes);
        String fileKey = imagePathPrefix.concat(userId.toString()).concat("/") // uid
                .concat(RandomUtil.randomString(5)).concat(".png"); //随机字符串5位，防止重名文件覆盖
        s3Tool.putPublicReadObject(fileKey, inputStream);
        return fileKey;
    }

    @Override
    public Object operationLogs() {
        Long userId = ReqInfoContext.getReqInfo().getUserId();
        List<ChatHistoryDO> historyList;
        try {
            historyList = chatHistoryDao.lambdaQuery()
                    .select(ChatHistoryDO::getId, ChatHistoryDO::getDesc,ChatHistoryDO::getCreateBy, ChatHistoryDO::getCreateTime, ChatHistoryDO::getUpdateTime, ChatHistoryDO::getResponse, ChatHistoryDO::getRequest)
                    .eq(ChatHistoryDO::getCreateBy, userId)
                    .orderByDesc(ChatHistoryDO::getCreateTime).list();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return Result.success(historyList);
    }

    @Override
    public Object removeLog(Long id) {
        Long userId = ReqInfoContext.getReqInfo().getUserId();
        try {
            ChatHistoryDO removeInfo= chatHistoryDao.lambdaQuery()
                    .eq(ChatHistoryDO::getId, id)
                    .eq(ChatHistoryDO::getCreateBy, userId)
                    .orderByAsc(ChatHistoryDO::getCreateTime)
                    .one();
            if (Objects.isNull(removeInfo)) {
                return Result.failure(ResultStatus.BAD_REQUEST.getCode(), "数据不存在", null, "Data does not exist");
            }
            //删除S3服务器文件
            JSONArray deleteArr = JSONArray.parseArray(removeInfo.getResponse());
            for (int i = 0; i < deleteArr.size(); i++) {
                String deleteKey = deleteArr.getJSONObject(i).getString("fileKey");
                if (StringUtils.isNotEmpty(deleteKey)) {
                    s3Tool.deleteObject(deleteKey);
                }
            }
            chatHistoryDao.lambdaUpdate().eq(ChatHistoryDO::getId, id).remove();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return Result.success("删除成功");
    }

    @Override
    public Object getSizeConfig() {
        ConfigEntity configInfo =  configRepository.get("image", "image_size");
        return JSONObject.parseObject(configInfo.getValue());
    }

    @Override
    public Object imageCategory(ImageCategoryReq req) {
        if (StringUtils.isEmpty(req.getAppId())) {
            req.setAppId(this.appId);
        }
        // 参数准备
        List<String> urls = new ArrayList<>();
        List<String> aks = new ArrayList<>();
        List<String> sks = new ArrayList<>();
        List<String> appIds = new ArrayList<>();
        List<String> serviceIds = new ArrayList<>();
        List<String> serviceNames = new ArrayList<>();
        if (Objects.nonNull(req.getId()) && !req.getId().isEmpty()) {
            for (Long id : req.getId()) {
                ServerDO serverDO = serverDao.getById(id);
                if (Objects.isNull(serverDO)) {
                    return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "服务不存在，请选择其它服务体验", null, "Service does not exist, please choose another service experience");
                }
                serviceIds.add(serverDO.getServiceId());
                appIds.add(serverDO.getAppId());
                urls.add(serverDO.getUrl());
                serviceNames.add(serverDO.getName());
                UserDTO userDTO = ReqInfoContext.getReqInfo().getUser();
                if (userDTO != null && userDTO.getSource() == 1) {
                    try {
                        JSONObject resultObject = iflyAICloudAppRemoteService.appInfo(serverDO.getAppId()).tryData();
                        if (resultObject != null) {
                            aks.add(resultObject.getString("apiKey"));
                            sks.add(resultObject.getString("apiSecret"));
                        } else {
                            if ("en".equals(language)) {
                                throw new CustomException("Failed to obtain application information");
                            }
                            throw new CustomException("获取应用信息失败");
                        }
                    } catch (RemoteException e) {
                        return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), e.getMessage());
                    }
                } else {
                    try {
                        JSONObject appInfo = aiCloudAppRemoteService.appInfo(serverDO.getAppId(), "iat").tryData();
                        aks.add(appInfo.getString("apiKey"));
                        sks.add(appInfo.getString("apiSecret"));
                    } catch (RemoteException e) {
                        return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), e.getMessage());
                    }
                }
            }
        } else {
            if (Objects.nonNull(req.getServiceId())) {
                CloudDTO cloudDTO = ReqInfoContext.getReqInfo().getCloudInfo();
                String cloudId = cloudDTO == null ? "0" : cloudDTO.getCloudId();
                BaseServerDO baseServerDO = baseServerDao.lambdaQuery().eq(BaseServerDO::getServiceId, req.getServiceId()).eq(BaseServerDO::getPatchId, req.getPatchId()).eq(BaseServerDO::getCloudId, cloudId).one();
                if (Objects.isNull(baseServerDO)) {
                    return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "服务不存在，请选择其它服务体验", null, "Service does not exist, please choose another service experience");
                }
                urls.add(baseServerDO.getUrl());
                serviceIds.add(req.getServiceId());
                serviceNames.add(baseServerDO.getServiceId());
                if (StringUtils.isNotEmpty(baseServerDO.getAppId())) {
                    appIds.add(baseServerDO.getAppId());
                    aks.add(baseServerDO.getApiKey());
                    sks.add(baseServerDO.getApiSecret());
                } else {
                    appIds.add(appId);
                    aks.add(appKey);
                    sks.add(appSecret);
                }
            } else {
                return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "服务不存在，请选择其它服务体验", null, "Service does not exist, please choose another service experience");
            }
        }
        if (Objects.isNull(req.getImages()) || req.getImages().isEmpty()) {
            return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "图像数据不能为空", null, "Image data cannot be empty");
        }
        JSONArray respArr = new JSONArray();
        for (String image : req.getImages()) {
            for (int i = 0; i < serviceIds.size(); i++) {
                JSONObject reqObj = JSONObject.parseObject("{\"header\":{\"app_id\":\"123456\",\"uid\":\"\",\"did\":\"\",\"imei\":\"\",\"imsi\":\"\",\"mac\":\"\",\"net_type\":\"wifi\",\"net_isp\":\"CMCC\",\"status\":3,\"request_id\":null,\"res_id\":\"\"},\"parameter\":{},\"payload\":{\"image\":{\"encoding\":\"jpg\",\"image\":\"\",\"status\":3}}}");
                reqObj.getJSONObject("header").put("app_id", appIds.get(i));
                JSONObject service = JSONObject.parseObject("{\"atp_patch_id\":\"0\",\"result\":{\"encoding\":\"utf8\",\"compress\":\"raw\",\"format\":\"plain\"}}");
                reqObj.getJSONObject("parameter").put(serviceIds.get(i), service);
                reqObj.getJSONObject("payload").getJSONObject("image").put("image", image);
                //调用推理接口
                Object resObject;
                try {
                    resObject = aseHttpUtil(urls.get(i), aks.get(i), sks.get(i), reqObj);
                } catch (IOException e) {
                    log.error(e.getMessage());
                    return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "获取图片分类结果失败，请稍后再试", null, "Failed to obtain image classification results, please try again later");
                }
                if (resObject == null) {
                    return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "获取图片分类结果失败，请稍后再试", null, "Failed to obtain image classification results, please try again later");
                }
                JSONObject responseJson = JSONObject.parseObject(resObject.toString());
                log.info("ChatServiceImpl.imageCategory resp : {}", responseJson);
                //推理成功
                JSONObject resp = new JSONObject();
                String code = responseJson.getJSONObject("header").getString("code");
                resp.put("code", code);
                resp.put("serviceName", serviceNames.get(i));
                if ("0".equals(code)) {
                    String data = responseJson.getJSONObject("payload").getJSONObject("result").getString("text");
                    try {
                        JSONObject dataJson = JSONObject.parseObject(cn.hutool.core.codec.Base64.decodeStr(data));
                        if (Objects.isNull(dataJson) || StringUtils.isEmpty(dataJson.getString("label"))) {
                            resp.put("data", data);
                        } else {
                            resp.put("data", cn.hutool.core.codec.Base64.encode(dataJson.getString("label")));
                        }
                    } catch (Exception e) {
                        resp.put("data", data);
                    }
                } else {
                    String msg = responseJson.getJSONObject("header").getString("message");
                    String sid = responseJson.getJSONObject("header").getString("sid");
                    resp.put("msg", msg);
                    log.info("推理接口响应异常: sid: {}, code: {}, msg: {}", sid, code, msg);
                    return Result.failure(ResultStatus.INTERNAL_SERVER_ERROR.getCode(), "获取图片分类结果失败，请稍后再试", null, "Failed to obtain image classification results, please try again later");
                }
                respArr.add(resp);
            }
        }
        return respArr;
    }

    @SuppressWarnings("deprecation")
    public Object aseHttpUtil(String url, String ak, String sk, JSONObject body) throws IOException {
        Hmac256Signature signature = new Hmac256Signature(ak, sk, url, "POST");
        HttpUrl.Builder urlBuilder = Objects.requireNonNull(HttpUrl.parse(url)).newBuilder();
        try {
            HttpUrl httpUrl = AuthUtil.generateAuthorizationHttpUrl(signature);
            Set<String> names = httpUrl.queryParameterNames();
            for (String name : names) {
                urlBuilder.addQueryParameter(name, httpUrl.queryParameter(name));
            }
        } catch (SignatureException | MalformedURLException e) {
            throw new RuntimeException(e);
        }
        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        Request request = new Request.Builder()
                .url(urlBuilder.build().toString())
                .post(RequestBody.create(mediaType, body.toJSONString()))
                .build();
        // 发送请求并处理响应
        Response response = okHttpClient.newCall(request).execute();
        String responseBody = null;
        if (response.body() != null) {
            responseBody = response.body().string();
        }
        response.close();
        return JSONObject.parseObject(responseBody);
    }
}
