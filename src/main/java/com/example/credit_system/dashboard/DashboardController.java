package com.example.credit_system.dashboard;

import com.example.credit_system.global.auth.SessionConst;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestAttribute;

@Controller
public class DashboardController {

    /** 로그인 사용자의 대시보드를 반환한다. */
    @GetMapping("/dashboard")
    public String dashboard(@RequestAttribute(SessionConst.USERNAME) String username, Model model) {
        model.addAttribute("username", username);
        return "dashboard";
    }
}
