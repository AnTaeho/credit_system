package com.example.credit_system.auth.controller;

import com.example.credit_system.auth.repository.UserRepository;
import com.example.credit_system.global.auth.SessionConst;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequiredArgsConstructor
public class LoginController {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    @GetMapping("/login")
    public String loginForm() {
        return "login";
    }

    @PostMapping("/login")
    public String login(@RequestParam String username,
                         @RequestParam String password,
                         HttpServletRequest request) {
        return userRepository.findByUsername(username)
                .filter(user -> passwordEncoder.matches(password, user.getPassword()))
                .map(user -> {
                    HttpSession session = request.getSession(true);
                    session.setAttribute(SessionConst.USER_ID, user.getId());
                    session.setAttribute(SessionConst.ORGANIZATION_ID, user.getOrganizationId());
                    session.setAttribute(SessionConst.USERNAME, user.getUsername());
                    return "redirect:/dashboard";
                })
                .orElse("redirect:/login?error");
    }
}
