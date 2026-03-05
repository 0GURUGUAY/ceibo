// @ts-nocheck
import { serve } from 'https://deno.land/std@0.224.0/http/server.ts';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS'
};

serve(async req => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }

  if (req.method !== 'POST') {
    return new Response(JSON.stringify({ error: 'Method not allowed' }), {
      status: 405,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }

  const anthropicApiKey = Deno.env.get('ANTHROPIC_API_KEY');
  if (!anthropicApiKey) {
    return new Response(JSON.stringify({ error: 'Missing ANTHROPIC_API_KEY secret' }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }

  try {
    const body = await req.json();
    const model = String(body?.model || '').trim();
    const prompt = String(body?.prompt || '').trim();
    const maxTokens = Number.isFinite(Number(body?.maxTokens)) ? Number(body.maxTokens) : 1200;
    const temperature = Number.isFinite(Number(body?.temperature)) ? Number(body.temperature) : 0.2;

    if (!model || !prompt) {
      return new Response(JSON.stringify({ error: 'Missing model or prompt' }), {
        status: 400,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    const anthropicResponse = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': anthropicApiKey,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model,
        max_tokens: Math.max(1, Math.min(4096, Math.floor(maxTokens))),
        temperature,
        messages: [{ role: 'user', content: prompt }]
      })
    });

    const payload = await anthropicResponse.json().catch(() => ({}));
    if (!anthropicResponse.ok) {
      return new Response(JSON.stringify(payload), {
        status: anthropicResponse.status,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    const blocks = Array.isArray(payload?.content) ? payload.content : [];
    const text = blocks.map((block: { text?: string }) => String(block?.text || '')).join('\n').trim();

    return new Response(JSON.stringify({ text }), {
      status: 200,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  } catch (error) {
    return new Response(JSON.stringify({ error: String(error?.message || error) }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
});
