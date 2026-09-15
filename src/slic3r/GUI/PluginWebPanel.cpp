#include "PluginWebPanel.hpp"

#include "GUI.hpp"
#include "GUI_App.hpp"
#include "Widgets/PluginWebContent.hpp"
#include "Widgets/WebView.hpp"
#include "Widgets/WebViewHostDialog.hpp"

#include <libslic3r/Utils.hpp>

#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>

#include <wx/sizer.h>

namespace Slic3r { namespace GUI {

PluginWebPanel::PluginWebPanel(wxWindow* parent, const char* bridge_script)
    : wxPanel(parent, wxID_ANY)
{
    SetBackgroundColour(wxGetApp().get_window_default_clr());
    auto* sizer = new wxBoxSizer(wxVERTICAL);
    SetSizer(sizer);

    // Never null: WebView::CreateWebView substitutes a placeholder view when no backend is available.
    const std::string bootstrap = (boost::filesystem::path(resources_dir()) / plugin_web::BOOTSTRAP_PAGE).make_preferred().string();
    m_browser = WebView::CreateWebView(this, wxString("file://") + from_u8(bootstrap));
    m_browser->SetBackgroundColour(GetBackgroundColour());
    m_browser->AddUserScript(wxString::FromUTF8(WebViewHostDialog::theme_user_script()));
    m_browser->AddUserScript(wxString::FromUTF8(WebViewHostDialog::plugin_defaults_user_script()));
    m_browser->AddUserScript(wxString::FromUTF8(bridge_script));
    m_browser->Bind(wxEVT_WEBVIEW_LOADED, &PluginWebPanel::on_load_event, this);
    m_browser->Bind(wxEVT_WEBVIEW_ERROR, &PluginWebPanel::on_load_event, this);
    m_browser->Bind(wxEVT_WEBVIEW_SCRIPT_MESSAGE_RECEIVED, &PluginWebPanel::on_script_message, this);
    m_browser->Bind(EVT_WEBVIEW_RECREATED, &PluginWebPanel::on_webview_recreated, this);
    sizer->Add(m_browser, 1, wxEXPAND);
}

void PluginWebPanel::on_load_event(wxWebViewEvent& event)
{
    if (!m_content_loaded) {
        // The first bootstrap load (or its error) triggers the swap to the plugin HTML.
        m_content_loaded = true;
        load_page_html();
    } else if (event.GetEventType() != wxEVT_WEBVIEW_LOADED) {
        // A failed load has no document to theme, but it does end the swap it belonged to: leaving
        // the flag set would make the next reload look like our own load and be swallowed.
        m_own_page_load = false;
    } else {
        // WebKit reloads the SetPage base URL rather than the injected page, and the injected
        // document reports that same URL, so a load of it that is not the swap we started is a
        // browser reload and the plugin HTML has to be put back. A page a link loaded arrives
        // under its own URL. The Edge backend ignores the base URL, so this never matches there.
        if (!m_own_page_load && plugin_web::is_content_url(event.GetURL())) {
            load_page_html();
        } else {
            m_own_page_load = false;
            // The document-start theme script keeps the theme the web view was created with, so
            // bring every later document (the plugin page, or one a link loaded) onto the app theme.
            apply_theme();
        }
    }
    event.Skip();
}

void PluginWebPanel::load_page_html()
{
    if (const std::optional<std::string> html = page_html()) {
        m_own_page_load = true;
        m_browser->SetPage(wxString::FromUTF8(*html), plugin_web::content_base_url());
    }
}

void PluginWebPanel::on_script_message(wxWebViewEvent& event)
{
    const nlohmann::json payload = nlohmann::json::parse(event.GetString().utf8_string(), nullptr, false);
    if (!payload.is_object() || payload.value("channel", std::string()) != "orca")
        return;

    const std::string kind = payload.value("kind", std::string());
    if (!on_page_message(kind, payload.contains("data") ? payload["data"] : nlohmann::json()))
        BOOST_LOG_TRIVIAL(warning) << "Plugin web panel ignored a window.orca '" << kind << "' call; this host does not support it";
}

void PluginWebPanel::on_webview_recreated(wxCommandEvent&)
{
    SetBackgroundColour(wxGetApp().get_window_default_clr());
    m_browser->SetBackgroundColour(GetBackgroundColour());
    Refresh();
    // Handled without Skip(), so WebView::RecreateAll() does not reload the plugin page.
    apply_theme();
}

void PluginWebPanel::apply_theme()
{
    WebView::RunScript(m_browser, wxString::FromUTF8(WebViewHostDialog::theme_apply_script()));
}

void PluginWebPanel::post_to_page(const std::string& json)
{
    WebView::RunScript(m_browser, wxString::Format(
        "(function dispatch(payload, attempts) {\n"
        "  if (typeof window.__orcaDispatch === 'function') { window.__orcaDispatch(payload); return; }\n"
        "  if (attempts < 100) window.setTimeout(function() { dispatch(payload, attempts + 1); }, 25);\n"
        "})({data: %s}, 0);",
        wxString::FromUTF8(json)));
}

}} // namespace Slic3r::GUI
