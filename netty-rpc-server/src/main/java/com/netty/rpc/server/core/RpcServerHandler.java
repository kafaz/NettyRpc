package com.netty.rpc.server.core;

import com.netty.rpc.codec.Beat;
import com.netty.rpc.codec.RpcRequest;
import com.netty.rpc.codec.RpcResponse;
import com.netty.rpc.util.ServiceUtil;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import net.sf.cglib.reflect.FastClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * RPC服务器处理器，负责处理客户端发来的RPC请求
 * <p>
 * 该处理器实现了Netty的SimpleChannelInboundHandler接口，专门用于处理RpcRequest类型的消息。
 * 它负责将接收到的RPC请求分发给相应的服务实现类，并将处理结果返回给客户端。
 * 同时处理心跳检测、异常情况和空闲连接管理。
 * </p>
 *
 * @author luxiaoxun
 */
public class RpcServerHandler extends SimpleChannelInboundHandler<RpcRequest> {

    private static final Logger logger = LoggerFactory.getLogger(RpcServerHandler.class);

    /**
     * 服务映射表，key为服务名+版本号，value为服务实现实例
     */
    private final Map<String, Object> handlerMap;
    
    /**
     * 服务处理线程池，用于异步处理RPC请求
     */
    private final ThreadPoolExecutor serverHandlerPool;

    /**
     * 构造RPC服务处理器
     *
     * @param handlerMap 服务实现映射表，用于查找服务实现类
     * @param threadPoolExecutor 线程池，用于异步处理请求
     */
    public RpcServerHandler(Map<String, Object> handlerMap, final ThreadPoolExecutor threadPoolExecutor) {
        this.handlerMap = handlerMap;
        this.serverHandlerPool = threadPoolExecutor;
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final RpcRequest request) {
        // 过滤心跳请求，心跳请求不需要返回响应
        if (Beat.BEAT_ID.equalsIgnoreCase(request.getRequestId())) {
            logger.info("Server read heartbeat ping");
            return;
        }

        // 将请求提交到线程池异步处理，避免阻塞IO线程
        serverHandlerPool.execute(new Runnable() {
            @Override
            public void run() {
                logger.info("Receive request " + request.getRequestId());
                // 创建响应对象
                RpcResponse response = new RpcResponse();
                // 设置请求ID，用于请求-响应匹配
                response.setRequestId(request.getRequestId());
                try {
                    // 调用本地服务处理请求
                    Object result = handle(request);
                    response.setResult(result);
                } catch (Throwable t) {
                    // 处理异常情况，将异常信息设置到响应中
                    response.setError(t.toString());
                    logger.error("RPC Server handle request error", t);
                }
                // 将响应写回客户端，并添加监听器记录日志
                ctx.writeAndFlush(response).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture channelFuture) throws Exception {
                        logger.info("Send response for request " + request.getRequestId());
                    }
                });
            }
        });
    }

    /**
     * 处理RPC请求的核心方法
     * <p>
     * 根据请求中的类名、版本号找到对应的服务实现，然后通过反射调用相应的方法。
     * 使用CGLIB进行方法调用以提高性能。
     * </p>
     *
     * @param request RPC请求对象，包含调用的服务名、方法名、参数等信息
     * @return 方法调用结果
     * @throws Throwable 调用过程中可能抛出的任何异常
     */
    private Object handle(RpcRequest request) throws Throwable {
        // 获取接口名称
        String className = request.getClassName();
        // 获取服务版本
        String version = request.getVersion();
        // 生成服务键，用于在handlerMap中查找服务实现
        String serviceKey = ServiceUtil.makeServiceKey(className, version);
        // 查找服务实现实例
        Object serviceBean = handlerMap.get(serviceKey);
        if (serviceBean == null) {
            logger.error("Can not find service implement with interface name: {} and version: {}", className, version);
            return null;
        }

        // 获取服务类信息
        Class<?> serviceClass = serviceBean.getClass();
        // 获取请求的方法名
        String methodName = request.getMethodName();
        // 获取方法参数类型数组
        Class<?>[] parameterTypes = request.getParameterTypes();
        // 获取方法参数值数组
        Object[] parameters = request.getParameters();

        // 记录详细调用日志（调试用）
        logger.debug(serviceClass.getName());
        logger.debug(methodName);
        for (int i = 0; i < parameterTypes.length; ++i) {
            logger.debug(parameterTypes[i].getName());
        }
        for (int i = 0; i < parameters.length; ++i) {
            logger.debug(parameters[i].toString());
        }

        // JDK 反射方式调用（已注释，使用CGLIB方式替代）
//        Method method = serviceClass.getMethod(methodName, parameterTypes);
//        method.setAccessible(true);
//        return method.invoke(serviceBean, parameters);

        // 使用CGLIB进行反射调用，性能更好
        FastClass serviceFastClass = FastClass.create(serviceClass);
//        FastMethod serviceFastMethod = serviceFastClass.getMethod(methodName, parameterTypes);
//        return serviceFastMethod.invoke(serviceBean, parameters);
        
        // 使用方法索引进行调用，性能更高
        int methodIndex = serviceFastClass.getIndex(methodName, parameterTypes);
        return serviceFastClass.invoke(methodIndex, serviceBean, parameters);
    }

    /**
     * 处理通道异常事件
     * <p>
     * 当通道发生异常时关闭连接，防止资源泄露
     * </p>
     *
     * @param ctx 通道上下文
     * @param cause 异常原因
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.warn("Server caught exception: " + cause.getMessage());
        // 关闭连接
        ctx.close();
    }

    /**
     * 处理用户自定义事件
     * <p>
     * 主要用于处理空闲连接检测事件，当连接空闲超时时关闭连接
     * </p>
     *
     * @param ctx 通道上下文
     * @param evt 事件对象
     * @throws Exception 可能的异常
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        // 检测到空闲连接事件
        if (evt instanceof IdleStateEvent) {
            // 关闭空闲连接，释放资源
            ctx.channel().close();
            logger.warn("Channel idle in last {} seconds, close it", Beat.BEAT_TIMEOUT);
        } else {
            // 其他类型事件交给父类处理
            super.userEventTriggered(ctx, evt);
        }
    }
}
