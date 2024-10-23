package tw.com.hanjiCHEN.auditservice.cofig;

import com.amazonaws.xray.entities.Subsegment;
import com.amazonaws.xray.spring.aop.BaseAbstractXRayInterceptor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

import java.util.Map;

@Aspect
@Component
public class XRayInspector extends BaseAbstractXRayInterceptor {

    @Override
    protected Map<String, Map<String, Object>> generateMetadata(
            ProceedingJoinPoint proceedingJoinPoint, Subsegment subsegment
    ) {
        return super.generateMetadata(proceedingJoinPoint, subsegment);
    }

    @Override
    //檢測所有使用此類(com.amazonaws.xray.spring.aop.XRayEnable)的內容
    @Pointcut("@within(com.amazonaws.xray.spring.aop.XRayEnable)")
    protected void xrayEnabledClasses() {
    }
}
