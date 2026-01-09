const downloadReport = async () => {
  if (!keycloak) {
    setError('Keycloak not ready');
    return;
  }

  try {
    setLoading(true);
    setError(null);

    // чтобы токен не протух
    await keycloak.updateToken(30);

    if (!keycloak.token) {
      setError('Not authenticated');
      return;
    }

    const response = await fetch(`${process.env.REACT_APP_API_URL}/reports?format=csv`, {
      headers: {
        Authorization: `Bearer ${keycloak.token}`,
      },
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`API error ${response.status}: ${text}`);
    }

    // скачиваем CSV как файл
    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = 'report.csv';
    document.body.appendChild(a);
    a.click();
    a.remove();

    window.URL.revokeObjectURL(url);
  } catch (err) {
    setError(err instanceof Error ? err.message : 'An error occurred');
  } finally {
    setLoading(false);
  }
};
