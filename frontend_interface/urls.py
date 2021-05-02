from django.urls import path
from django.contrib.auth import views as auth_views

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('daddy/', views.daddy_interface, name='daddy_interface'),
    path('eth_daddy', views.eth_daddy_interface, name='eth_daddy_interface'),
    path('vol_trend/', views.vol_trend_interface, name='vol_trend'),
    path('altcoin/', views.altcoin_interface, name='altcoin_interface'),
    path('ratio/', views.ratio_interface, name='ratio_interface'),
    path('chadlor/', views.chadlor_interface, name='chadlor_interface'),
    path('interface/', views.interface, name='interface'),
    path('add_parms/', views.addParms, name='addParms'),
    path('clear_log/', views.clearLog, name='clearLog'),
    path('reverse_status/', views.reverse_status, name='reverse_status'),
    path('delete/', views.delete, name='delete'),
    path('download/', views.csv_downloader, name='download'),
    path('trades/', views.show_trades, name='trades'),
    path('login/', auth_views.LoginView.as_view(), name='login'),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),
    path('nissan/', views.nissan, name='nissan')
]