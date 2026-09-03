export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    async function tg(method, data) {
      const r = await fetch(
        `https://api.telegram.org/bot${env.BOT_TOKEN}/${method}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(data)
        }
      );
      return r.json();
    }

    if (request.method === "GET" && url.pathname === "/") {
      return new Response("Lexxi Admin Bot is running.");
    }

    if (
      request.method === "GET" &&
      url.pathname === "/setup" &&
      url.searchParams.get("key") === env.SETUP_KEY
    ) {
      return Response.json(
        await tg("setWebhook", {
          url: `${url.origin}/webhook`,
          allowed_updates: ["chat_join_request"]
        })
      );
    }

    if (request.method === "POST" && url.pathname === "/webhook") {
      const update = await request.json();
      const join = update?.chat_join_request;

      if (!join) return new Response("OK");

      await tg("sendMessage", {
        chat_id: join.user_chat_id,
        text: "👋 Welcome!\n\nThanks for joining our channel. ❤️"
      });

      await tg("approveChatJoinRequest", {
        chat_id: join.chat.id,
        user_id: join.from.id
      });

      return new Response("OK");
    }

    return new Response("Not Found", { status: 404 });
  }
};
