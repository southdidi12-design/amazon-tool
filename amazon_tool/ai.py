import json
import re

try:
    import openai

    HAS_OPENAI = True
except Exception:
    HAS_OPENAI = False


def deepseek_audit(api_key, row, proposed_value, rule_reason, is_star, value_label):
    if not HAS_OPENAI or not api_key:
        return "未审核", proposed_value
    try:
        client = openai.OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
        role = "【⭐主推款】(抢排名策略)" if is_star else "【普通款】(保利润策略)"
        prompt = f"""
        我是亚马逊运营。
        对象：{row['campaign_name']} ({role})
        数据：花费${row['cost']}, ACOS {row['acos']*100:.1f}%。
        算法建议：{value_label}调整为 ${proposed_value} (理由: {rule_reason})
        请审核该建议是否合理？
        返回 JSON: {{"comment": "理由", "final_value": 数字}}
        """
        response = client.chat.completions.create(
            model="deepseek-chat", messages=[{"role": "user", "content": prompt}], temperature=0.1
        )
        res = json.loads(re.search(r"\{.*\}", response.choices[0].message.content, re.DOTALL).group())
        final_value = res.get("final_value", res.get("final_bid", proposed_value))
        comment = res.get("comment", "")
        return f"AI: {comment}".strip(), float(final_value)
    except Exception as e:
        return f"AI报错: {str(e)}", proposed_value


def deepseek_relevance(api_key, campaign_name, positive_terms, search_term):
    if not HAS_OPENAI or not api_key:
        return None, "AI未配置/或缺少库"
    try:
        client = openai.OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
        context_terms = [t for t in positive_terms if t][:20]
        context = "、".join(context_terms) if context_terms else "无"
        prompt = f"""
你是亚马逊广告优化助手，请判断搜索词是否与当前商品/投放相关。
已知相关词（来自最近转化的搜索词）：{context}
活动名：{campaign_name or "未知"}
搜索词：{search_term}
只返回 JSON：{{"relevant": true/false, "reason": "原因"}}
"""
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        raw = response.choices[0].message.content
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            return None, "AI无有效JSON"
        data = json.loads(match.group())
        relevant = data.get("relevant", data.get("is_relevant"))
        if isinstance(relevant, str):
            relevant = relevant.strip().lower() in ["true", "yes", "1"]
        if isinstance(relevant, bool):
            return relevant, str(data.get("reason", "") or "")
        return None, "AI返回异常格式"
    except Exception as exc:
        return None, f"AI请求失败: {exc}"
