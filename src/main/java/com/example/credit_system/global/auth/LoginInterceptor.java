package com.example.credit_system.global.auth;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.io.IOException;

@Component
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws IOException {
        HttpSession session = request.getSession(false);
        Object organizationId = session == null ? null : session.getAttribute(SessionConst.ORGANIZATION_ID);

        if (organizationId == null) {
            if (request.getRequestURI().startsWith("/api/")) {
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            } else {
                response.sendRedirect("/login");
            }
            return false;
        }

        request.setAttribute(SessionConst.ORGANIZATION_ID, organizationId);
        request.setAttribute(SessionConst.USER_ID, session.getAttribute(SessionConst.USER_ID));
        request.setAttribute(SessionConst.USERNAME, session.getAttribute(SessionConst.USERNAME));
        return true;
    }
}
